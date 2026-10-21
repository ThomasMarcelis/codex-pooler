defmodule CodexPoolerWeb.Runtime.BackendCodexMailboxSessionReplacementTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, ClientRetry, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.Accounting.RequestLifecycle.FailedPredecessorResend
  alias CodexPooler.Dev.NativeCompactionTrace.SensitivityRestorer
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.{BridgeSessionAlias, RuntimeCleanup}
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionTrace
  alias CodexPooler.Repo
  alias CodexPoolerWeb.GatewayControllerHelpers
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @detection_budget 15_000

  @negative_mutations [:legacy_reason, :nonexpiry_close, :old_token_close, :earlier_creation, :foreign_key, :wrong_model, :epoch, :changed_prefix, :retry_window]
  # A comprehension expands and compiles a test's body once per generated test, so a loop that generates more than a few tests keeps
  # the scenario in a private function below it and each generated test is one call.
  for mode <- ["full", "lite"], {carrier, state} <- [{:http, :same_session}, {:http, :replacement_fresh}, {:http, :replacement_engaged}, {:ws_direct, :replacement_fresh}, {:ws_owner, :replacement_fresh}, {:http, :window_successor}, {:ws_direct, :window_successor}, {:ws_owner, :window_successor}] ++ Enum.map(@negative_mutations, &{:http, {:negative, &1}}) do
    replacement? = state not in [:same_session, :window_successor]

    mutation =
      case state do
        {:negative, mutation} -> mutation
        _positive -> nil
      end

    if replacement? and mutation != :nonexpiry_close do
      @tag slow: "observes a real one-second PostgreSQL lease expiry after completed HTTP settlement and heartbeat shutdown"
    end

    if mutation, do: @tag(mailbox_replacement_negative: true)
    if state == :window_successor, do: @tag(mailbox_window: true)
    @tag window_successor?: state == :window_successor
    @tag mode: mode, carrier: carrier, replacement?: replacement?, engaged?: state == :replacement_engaged, mutation: mutation
    test "#{mode} #{carrier} mailbox continuation #{inspect(state)}", context do
      assert_mailbox_continuation!(context)
    end
  end

  # Reason: the body of a generated test; its branches select the matrix case.
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp assert_mailbox_continuation!(context) do
    # provenance: synthetic_adversarial; positive expiry cases use HTTP
    # settlement, real lease renewal and a database-observed deadline crossing.
    # The nonexpiry control seeds idle retirement inputs for the actual cleanup writer.
    output = %{"type" => "reasoning", "id" => "rs_synthetic_mailbox", "summary" => [], "encrypted_content" => "synthetic_reasoning"}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_mailbox_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    first_stream = FakeUpstream.sse_stream([{"response.output_item.done", %{"type" => "response.output_item.done", "item" => output}}, {"response.completed", completed}])
    remaining = List.duplicate(FakeUpstream.sse_stream([{"response.completed", completed}]), if(context.engaged?, do: 2, else: 1))
    upstream = start_upstream(FakeUpstream.strict_sequence([first_stream | remaining]))
    setup = gateway_setup(upstream, compact?: true)
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.carrier == :ws_owner)
    set_model_serving_mode!(model_serving_scope(), setup, context.mode)
    thread = Ecto.UUID.generate()
    Process.put({__MODULE__, :include_turn_state_header}, context.carrier != :http and not context.window_successor?)
    Process.put({__MODULE__, :window_successor?}, context.window_successor?)
    input = native_text_input("synthetic")
    input = if context.window_successor?, do: input ++ [%{"type" => "compaction", "encrypted_content" => "synthetic-pivot"}], else: input
    payload = payload(setup, thread, input)

    Process.put({GatewayControllerHelpers, :owner_liveness_test_options}, %{bridge_owner_lease_ttl_seconds: 30, session_lease_heartbeat_test_observer: self()})
    first_response = send_request(setup, thread, payload, context.mode)
    assert first_response.status == 200
    assert_receive {:session_lease_heartbeat, :started, heartbeat}, @detection_budget
    monitor = Process.monitor(heartbeat)
    assert_receive {:session_lease_heartbeat, :stopped, ^heartbeat}, @detection_budget
    assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _reason}, @detection_budget

    first = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id)
    attempt = Repo.get_by!(Attempt, request_id: first.id)
    turn = Repo.get_by!(CodexTurn, request_id: first.id)
    session = Repo.get!(CodexSession, turn.codex_session_id)
    assert {first.status, attempt.status, turn.status} == {"succeeded", "succeeded", "succeeded"}
    assert attempt.transport == "http_sse"
    assert first.request_metadata["native_http_claim_arm"] == if(context.window_successor?, do: "post_compaction_resume", else: "opening")
    assert get_in(attempt.response_metadata, ["native_http_mailbox_prefix", "output_item_done_count"]) == 1
    assert_settled_once!(first.id)

    if context.mutation == :nonexpiry_close do
      # Retired-session cleanup is a real nonexpiry close writer. Its inputs
      # are an idle, alias-free fixture whose lease has already been retired.
      ttl = OperationalSettings.current().expired_alias_ttl_seconds
      {:ok, %{rows: [[now]]}} = Repo.query("SELECT clock_timestamp()")
      Repo.delete_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id))
      Repo.delete_all(from(a in BridgeSessionAlias, where: a.codex_session_id == ^session.id))
      Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [owner_lease_expires_at: DateTime.add(now, -ttl - 1, :second)])
      assert {:ok, %{closed_retired_sessions: 1}} = RuntimeCleanup.cleanup_expired(now)
      closed = Repo.get!(CodexSession, session.id)
      assert closed.status == "closed"
      assert is_nil(closed.close_reason)
    else
      if context.replacement? do
        # Only the idle, completed fixture lease is shortened through the real
        # renewal API. The request's pre-dispatch acquisition retains 30 seconds.
        options = RequestOptions.build([bridge_owner_lease_ttl_seconds: 1], @path, %{})
        assert {:ok, renewed} = SessionContinuity.renew_owner_token(session, session.owner_lease_token, options)
        lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: session.id, status: "active")
        assert renewed.owner_lease_expires_at == lease.expires_at
        assert DateTime.diff(lease.expires_at, lease.renewed_at, :microsecond) == 1_000_000
        await_expiry!(session.id, System.monotonic_time(:millisecond) + @detection_budget)
      end
    end

    control =
      if context.engaged? do
        document = payload["client_metadata"]["x-codex-turn-metadata"] |> CodexPooler.JSON.decode!() |> Map.put("turn_id", "synthetic_independent_control") |> CodexPooler.JSON.encode!()
        independent = %{payload | "input" => native_text_input("synthetic independent control"), "client_metadata" => %{"x-codex-turn-metadata" => document}}
        assert send_request(setup, thread, independent, context.mode).status == 200
        control = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id)
        control_turn = Repo.get_by!(CodexTurn, request_id: control.id)
        assert control_turn.codex_session_id != session.id
        assert control_turn.status == "succeeded"
        refute Map.has_key?(control.request_metadata, "client_resend")
        assert_settled_once!(control.id)
        control
      end

    mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
    continuation = Map.put(payload, "input", input ++ [Map.put(output, "content", nil), mailbox])

    continuation =
      if context.mutation do
        {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
        assert {:ok, replacement} = SessionContinuity.start_codex_session(auth, RequestOptions.for_websocket(%{session_key: session.session_key}))
        assert replacement.id != session.id
        mutate_replacement!(context.mutation, setup, session, replacement, first, continuation)
      else
        continuation
      end

    continuation =
      if context.window_successor? do
        # A websocket reconnect opens the next window before HTTP fallback
        # reaches it; the still-owned predecessor must remain a separate session.
        {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
        options = RequestOptions.for_websocket(%{session_header: "#{thread}:1", session_header_source: "x-codex-window-id"})
        assert {:ok, next_session} = SessionContinuity.start_codex_session(auth, options)
        refute next_session.id == session.id
        document = payload["client_metadata"]["x-codex-turn-metadata"] |> CodexPooler.JSON.decode!() |> Map.merge(%{"window_id" => "#{thread}:1", "window_number" => 1}) |> CodexPooler.JSON.encode!()
        put_in(continuation, ["client_metadata", "x-codex-turn-metadata"], document)
      else
        continuation
      end

    before = pool_counts(setup.pool.id)
    trace = start_admission_trace!()
    {response, logs} = with_info_log(fn -> send_carrier_request(setup, thread, continuation, context.mode, context.carrier) end)
    calls = stop_admission_trace!(trace)
    assert Map.get(calls, {FailedPredecessorResend, :resolve, 2}, 0) >= 1

    if context.mutation == :wrong_model do
      assert Map.get(calls, {ClientRetry, :mailbox_check_for_session, 7}, 0) == 0
    else
      assert Map.get(calls, {ClientRetry, :mailbox_check_for_session, 7}, 0) >= 1
    end

    assert Map.get(calls, {ClientRetry, :preflight_snapshot, 4}, 0) == 0
    assert Map.get(calls, {ClientRetry, :lock_eligible_predecessor!, 4}, 0) == 0
    status = carrier_status(response)
    await_completed_pool!(setup.pool.id, System.monotonic_time(:millisecond) + @detection_budget)
    old_session = Repo.get!(CodexSession, session.id)
    sessions = Repo.all(from s in CodexSession, where: s.pool_id == ^setup.pool.id)

    if context.replacement? do
      assert old_session.status == "closed"
      assert DateTime.compare(old_session.owner_lease_expires_at, old_session.closed_at) in [:lt, :eq]
      assert length(sessions) == 2
      replacement = Enum.find(sessions, &(&1.id != session.id))
      assert replacement.session_key == session.session_key
      if context.mutation == :earlier_creation, do: assert(DateTime.compare(replacement.created_at, old_session.closed_at) == :lt), else: assert(DateTime.compare(replacement.created_at, old_session.closed_at) in [:gt, :eq])
    else
      assert length(sessions) == if(context.window_successor?, do: 2, else: 1)
      assert old_session.status == "active"
      assert is_nil(old_session.close_reason)
    end

    terminal_predecessor? = String.contains?(logs, "resend_disposition=terminal_predecessor")

    if status == 409 do
      if is_nil(context.mutation), do: assert(terminal_predecessor?)
      assert FakeUpstream.count(upstream) == 1
      assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id), :count) == 0
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 1
      assert_settled_once!(first.id)
    end

    CodexPooler.TestDiagnostics.puts(fn ->
      CodexPooler.JSON.encode!(%{scenario: "mailbox_session_replacement", carrier: context.carrier, mode: context.mode, mutation: context.mutation, replacement: context.replacement?, engaged: context.engaged?, actual_expiry_observed: context.replacement? and context.mutation != :nonexpiry_close, nonexpiry_writer: if(context.mutation == :nonexpiry_close, do: "retired_session_cleanup"), status: status, terminal_predecessor: terminal_predecessor?, sessions: length(sessions), physical_dispatches: FakeUpstream.count(upstream), original_settlements: 1, admission_calls: Enum.map(calls, fn {{module, function, arity}, count} -> %{module: Atom.to_string(module), function: Atom.to_string(function), arity: arity, count: count} end)})
    end)

    if context.mutation do
      assert status == 409
      assert pool_counts(setup.pool.id) == before
      assert FakeUpstream.count(upstream) == 1

      stage =
        case context.mutation do
          :epoch -> "authorization"
          :changed_prefix -> "witness"
          :retry_window -> "verified"
          _authority -> "session"
        end

      assert logs =~ "mailbox_check=#{stage}"
    else
      assert status == 200, "mailbox admission must succeed after verified output and session lifecycle; terminal_predecessor=#{terminal_predecessor?}"
      refute terminal_predecessor?
      admitted = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and fragment("?->'client_resend'->>'predecessor_request_id'", r.request_metadata) == ^first.id)
      admitted_turn = Repo.get_by!(CodexTurn, request_id: admitted.id)
      assert admitted.status == "succeeded"
      refute inspect(admitted.request_metadata) =~ "#{thread}:1"
      assert admitted_turn.codex_session_id != session.id == (context.replacement? or context.window_successor?)
      assert admitted.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^admitted.id), :count) == 1
      assert_settled_once!(admitted.id)
      expected_dispatches = if control, do: 3, else: 2
      assert FakeUpstream.count(upstream) == expected_dispatches
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == expected_dispatches
      assert Repo.aggregate(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id), :count) == expected_dispatches
      assert Repo.aggregate(from(l in LedgerEntry, join: r in Request, on: r.id == l.request_id, where: r.pool_id == ^setup.pool.id and l.entry_kind == "settlement"), :count) == expected_dispatches
    end
  end

  for mode <- ["full", "lite"] do
    @tag mailbox_replacement_negative: true
    @tag slow: "observes real lease expiry and holds unrelated replacement generation at a provider frame barrier"
    test "#{mode} active unrelated replacement work is refused without an extra dispatch" do
      assert_active_unrelated_work_refused!(unquote(mode))
    end
  end

  defp assert_active_unrelated_work_refused!(mode) do
    output = %{"type" => "reasoning", "id" => "rs_synthetic_active", "summary" => [], "encrypted_content" => "synthetic_reasoning"}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_active_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    ref = make_ref()
    first_stream = FakeUpstream.sse_stream([{"response.output_item.done", %{"type" => "response.output_item.done", "item" => output}}, {"response.completed", completed}])
    control_stream = FakeUpstream.barrier_sse_stream([{"response.created", %{"type" => "response.created", "response" => %{"id" => "resp_synthetic_control"}}}, {"response.completed", completed}], notify: self(), release_ref: ref, barrier_after: 1)
    upstream = start_upstream(FakeUpstream.strict_sequence([first_stream, control_stream]))
    setup = gateway_setup(upstream, compact?: true)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    input = native_text_input("synthetic")
    opening = payload(setup, thread, input)
    Process.put({GatewayControllerHelpers, :owner_liveness_test_options}, %{bridge_owner_lease_ttl_seconds: 30, session_lease_heartbeat_test_observer: self()})
    assert send_request(setup, thread, opening, mode).status == 200
    assert_receive {:session_lease_heartbeat, :started, heartbeat}, @detection_budget
    monitor = Process.monitor(heartbeat)
    assert_receive {:session_lease_heartbeat, :stopped, ^heartbeat}, @detection_budget
    assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _}, @detection_budget
    original = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id)
    old_turn = Repo.get_by!(CodexTurn, request_id: original.id)
    old = Repo.get!(CodexSession, old_turn.codex_session_id)
    assert {:ok, _} = SessionContinuity.renew_owner_token(old, old.owner_lease_token, RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: 1}))
    await_expiry!(old.id, System.monotonic_time(:millisecond) + @detection_budget)
    document = opening["client_metadata"]["x-codex-turn-metadata"] |> CodexPooler.JSON.decode!() |> Map.put("turn_id", "synthetic_live_control") |> CodexPooler.JSON.encode!()
    control_payload = %{opening | "input" => native_text_input("synthetic unrelated live control"), "client_metadata" => %{"x-codex-turn-metadata" => document}}
    control = Task.async(fn -> send_request(setup, thread, control_payload, mode) end)
    control_monitor = Process.monitor(control.pid)
    on_exit(fn -> if Process.alive?(control.pid), do: Process.exit(control.pid, :kill) end)
    assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^ref}, @detection_budget
    on_exit(fn -> send(handler, {:fake_upstream_release_chunk, ref}) end)
    live = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^original.id)
    live_turn = Repo.get_by!(CodexTurn, request_id: live.id)
    assert live_turn.codex_session_id != old.id
    assert live_turn.status == "in_progress"
    before = pool_counts(setup.pool.id)
    mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
    continuation = %{opening | "input" => input ++ [output, mailbox]}
    {refusal, logs} = with_info_log(fn -> send_request(setup, thread, continuation, mode) end)
    assert refusal.status == 409
    assert logs =~ "mailbox_check=session"
    assert pool_counts(setup.pool.id) == before
    assert FakeUpstream.count(upstream) == 2
    send(handler, {:fake_upstream_release_chunk, ref})
    assert Task.await(control, @detection_budget).status == 200
    assert_receive {:DOWN, ^control_monitor, :process, _, :normal}, @detection_budget
    assert_settled_once!(original.id)
    assert_settled_once!(live.id)
    assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^original.id), :count) == 0
  end

  defp start_admission_trace! do
    patterns = [{FailedPredecessorResend, :resolve, 2}, {FailedPredecessorResend, :resolve_execution, 3}, {ClientRetry, :mailbox_check_for_session, 7}, {ClientRetry, :preflight_snapshot, 4}, {ClientRetry, :lock_eligible_predecessor!, 4}, {ClientRetry, :forwarded_chain_state, 1}, {Service, :prepare_replay_intent, 3}]
    tracer = spawn(fn -> admission_trace_loop(%{}) end)
    generation = make_ref()
    authorization = make_ref()
    # The controller only enrolls synthetic actors for on-actor sensitivity
    # restoration. No full-event telemetry handler or raw collector is installed.
    assert :ok = NativeCompactionTrace.activate_mode(:full)
    assert {:ok, restorer} = SensitivityRestorer.start(generation, authorization)
    assert :ok = SensitivityRestorer.bind_collector(tracer, generation, authorization)
    assert :ok = NativeCompactionTrace.activate_sensitivity_control(generation, authorization, restorer, tracer)
    parent = self()

    on_exit(fn ->
      if Process.alive?(restorer), do: SensitivityRestorer.stop_and_restore(generation, authorization)
      NativeCompactionTrace.deactivate_mode()
      Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local]))
      :erlang.trace(:new_processes, false, [:call])
      if Process.alive?(parent), do: :erlang.trace(parent, false, [:call])
      if Process.alive?(tracer), do: Process.exit(tracer, :kill)
    end)

    Enum.each(patterns, fn {module, _, _} = mfa ->
      Code.ensure_loaded!(module)
      :erlang.trace_pattern(mfa, true, [:local])
    end)

    :erlang.trace(parent, true, [:call, :arity, {:tracer, tracer}])
    :erlang.trace(:new_processes, true, [:call, :arity, {:tracer, tracer}])
    {tracer, patterns, generation, authorization}
  end

  defp stop_admission_trace!({tracer, patterns, generation, authorization}) do
    assert {:ok, restoration} = SensitivityRestorer.stop_and_restore(generation, authorization)
    assert Enum.all?(restoration, fn {_pid, receipt} -> receipt.state in [:restored, :dead] end)
    NativeCompactionTrace.deactivate_mode()
    Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local]))
    :erlang.trace(:new_processes, false, [:call])
    :erlang.trace(self(), false, [:call])
    delivered = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^delivered}, @detection_budget
    ref = make_ref()
    send(tracer, {:counts, self(), ref})
    assert_receive {:admission_calls, ^ref, counts}, @detection_budget
    counts
  end

  defp admission_trace_loop(counts) do
    receive do
      {:trace, _pid, :call, mfa} -> admission_trace_loop(Map.update(counts, mfa, 1, &(&1 + 1)))
      {:counts, parent, ref} -> send(parent, {:admission_calls, ref, counts})
    end
  end

  defp mutate_replacement!(:legacy_reason, _setup, old, _replacement, _request, payload) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^old.id), set: [close_reason: nil])
    payload
  end

  defp mutate_replacement!(:nonexpiry_close, _setup, old, _replacement, _request, payload) do
    assert %{status: "closed", close_reason: nil} = Repo.get!(CodexSession, old.id)
    payload
  end

  defp mutate_replacement!(:old_token_close, _setup, old, _replacement, _request, payload) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^old.id), set: [owner_lease_token: Ecto.UUID.generate()])
    assert is_nil(Repo.get!(CodexSession, old.id).close_reason)
    payload
  end

  defp mutate_replacement!(:earlier_creation, _setup, old, replacement, _request, payload) do
    closed = Repo.get!(CodexSession, old.id)
    Repo.update_all(from(s in CodexSession, where: s.id == ^replacement.id), set: [created_at: DateTime.add(closed.closed_at, -1, :microsecond)])
    payload
  end

  defp mutate_replacement!(:foreign_key, setup, old, _replacement, _request, payload) do
    %{api_key: key} = CodexPooler.PoolerFixtures.active_api_key_fixture(setup.pool)
    Repo.update_all(from(s in CodexSession, where: s.id == ^old.id), set: [api_key_id: key.id])
    payload
  end

  defp mutate_replacement!(:wrong_model, setup, _old, _replacement, request, payload) do
    model = CodexPooler.PoolerFixtures.model_fixture(setup.pool)
    Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [model_id: model.id])
    payload
  end

  defp mutate_replacement!(:epoch, setup, _old, _replacement, _request, payload) do
    Repo.update_all(from(k in CodexPooler.Access.APIKey, where: k.id == ^setup.api_key.id), inc: [runtime_revocation_epoch: 1])
    payload
  end

  defp mutate_replacement!(:changed_prefix, _setup, _old, _replacement, _request, payload), do: Map.update!(payload, "input", &List.replace_at(&1, 0, hd(native_text_input("synthetic changed prefix"))))

  defp mutate_replacement!(:retry_window, _setup, _old, _replacement, request, payload) do
    Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [completed_at: DateTime.add(DateTime.utc_now(), -31, :second)])
    payload
  end

  defp pool_counts(pool_id) do
    requests = from r in Request, where: r.pool_id == ^pool_id, select: r.id
    %{requests: Repo.aggregate(requests, :count), attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in subquery(requests)), :count), turns: Repo.aggregate(from(t in CodexTurn, where: t.request_id in subquery(requests)), :count), links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in subquery(requests)), :count), ledger: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in subquery(requests)), :count)}
  end

  defp payload(setup, thread, input) do
    %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})}}
  end

  defp send_request(setup, thread, payload, mode) do
    conn =
      build_conn()
      |> auth(setup)
      |> put_req_header("session-id", thread)
      |> put_req_header("thread-id", thread)
      |> put_req_header("x-codex-window-id", payload_window(payload))
      |> put_req_header("x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"])
      |> put_req_header("originator", "codex_cli_rs")

    conn = if Process.get({__MODULE__, :include_turn_state_header}), do: put_req_header(conn, "x-codex-turn-state", thread), else: conn
    conn = if mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @path, payload)
  end

  defp send_carrier_request(setup, thread, payload, mode, :http), do: send_request(setup, thread, payload, mode)

  defp send_carrier_request(setup, thread, payload, mode, _websocket) do
    port = start_public_endpoint!()
    before = WebsocketCleanupFence.listener_sockets()
    headers = [{"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", payload_window(payload)}, {"originator", "codex_cli_rs"}]
    headers = if mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers

    {conn, websocket, ref, _} =
      if Process.get({__MODULE__, :window_successor?}) do
        connect_window_socket!(port, setup, headers)
      else
        public_websocket_connect_with_request_headers!(port, setup, thread, @path, headers)
      end

    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    enrollment = observe_carrier_task!(socket)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.put(payload, "type", "response.create")))
    {task, monitor} = await_carrier_enrollment!(socket, enrollment)
    terminal = receive_terminal!(conn, websocket, ref, System.monotonic_time(:millisecond) + @detection_budget)
    await_carrier_task!(task, monitor)
    Mint.HTTP.close(conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
    terminal
  end

  defp payload_window(payload), do: payload["client_metadata"]["x-codex-turn-metadata"] |> CodexPooler.JSON.decode!() |> Map.fetch!("window_id")

  defp connect_window_socket!(port, setup, headers) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @path, [{"authorization", setup.authorization} | headers])
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref, response_headers}
  end

  defp observe_carrier_task!(socket) do
    ref = make_ref()
    parent = self()
    on_exit(fn -> send(socket, {ref, :release}) end)
    :ok = :sys.install(socket, {ref, &__MODULE__.carrier_task_probe/3, %{parent: parent, ref: ref}})
    ref
  end

  @doc false
  def carrier_task_probe(%{parent: parent, ref: ref}, {:in, {:direct_request_cleanup, task, _binding, _receipt}}, _name) do
    send(parent, {ref, :carrier_task, task})

    receive do
      {^ref, :release} -> :done
    after
      @detection_budget -> raise "carrier task enrollment was not released"
    end
  end

  def carrier_task_probe(probe, _event, _name), do: probe

  defp await_carrier_enrollment!(socket, ref) do
    receive do
      {^ref, :carrier_task, task} ->
        monitor = CodexPooler.TestProcess.monitor_flushed(task)
        send(socket, {ref, :release})
        {task, monitor}
    after
      @detection_budget -> flunk("actual carrier response task was not admitted")
    end
  end

  defp await_carrier_task!(task, monitor) do
    receive do
      {:DOWN, ^monitor, :process, ^task, :normal} -> :ok
    after
      @detection_budget -> flunk("actual carrier response task did not finish normally")
    end
  end

  defp receive_terminal!(conn, websocket, ref, deadline) do
    assert System.monotonic_time(:millisecond) < deadline, "websocket terminal was not received"
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(frame)
    if frame["type"] in ["response.completed", "error", "response.failed"], do: frame, else: receive_terminal!(conn, websocket, ref, deadline)
  end

  defp carrier_status(%Plug.Conn{status: status}), do: status
  defp carrier_status(%{"type" => "response.completed"}), do: 200
  defp carrier_status(%{"type" => "error", "error" => %{"code" => "duplicate_turn"}}), do: 409
  defp carrier_status(_other), do: flunk("unexpected websocket terminal class")

  defp await_completed_pool!(pool_id, deadline) do
    if Repo.exists?(from r in Request, where: r.pool_id == ^pool_id and r.status in ["accepted", "in_progress"]) do
      assert System.monotonic_time(:millisecond) < deadline, "owned requests did not settle"
      Process.sleep(20)
      await_completed_pool!(pool_id, deadline)
    end
  end

  defp assert_settled_once!(request_id) do
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request_id), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request_id and l.entry_kind == "settlement"), :count) == 1
  end

  defp await_expiry!(session_id, deadline) do
    expired? = Repo.one!(from s in CodexSession, where: s.id == ^session_id, select: fragment("? <= clock_timestamp()", s.owner_lease_expires_at))

    cond do
      expired? ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("owned lease did not reach its actual PostgreSQL expiry")

      true ->
        receive do
        after
          20 -> await_expiry!(session_id, deadline)
        end
    end
  end
end
