defmodule CodexPooler.Accounting.MailboxResumeChainTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, ClientRetry, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.Gateway.Payloads.{NativeMailboxContinuation, NativeTurnContinuation, RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn, SessionContinuity}
  alias CodexPooler.Repo

  @endpoint "/backend-api/codex/responses"

  setup do
    setup = accounting_setup()
    session = insert_session!(setup)
    semantic = :crypto.strong_rand_bytes(32)
    payload = payload(setup.model.exposed_model_id)
    {:post_compaction_resume, anchor} = NativeTurnContinuation.turn_role(payload)
    claim = WebsocketTurnIdentity.resume_claim_key(semantic, anchor)
    %{fixture: %{setup: setup, session: session, semantic: semantic, claim: claim, payload: payload}}
  end

  for predecessor_transport <- ["websocket", "http_sse"], successor_transport <- ["websocket", "http_sse"] do
    @tag mailbox_window: true
    test "a cut #{predecessor_transport} turn continues in the next window through #{successor_transport}", %{fixture: fixture} do
      assert_window_successor(fixture, unquote(predecessor_transport), unquote(successor_transport))
    end
  end

  for successor_transport <- ["websocket", "http_sse"] do
    test "two mailbox cuts traverse historical edges through #{successor_transport} successors", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, "websocket")
      first_output = reasoning("first")
      cut!(fixture, original, first_output)
      first_payload = append(fixture.payload, [first_output | Enum.map(1..5, &mailbox/1)])
      first = admit!(fixture, first_payload, unquote(successor_transport))
      assert_edge!(original, first)

      second_output = reasoning("second")
      cut!(fixture, first, second_output)
      expire!(original)
      second_payload = append(first_payload, [second_output, mailbox(6)])
      second = admit!(fixture, second_payload, unquote(successor_transport))
      assert_edge!(first, second)

      assert original.correlation_id == fixture.claim
      assert claim_for(fixture, first_payload) == fixture.claim
      assert claim_for(fixture, second_payload) == fixture.claim
      assert counts(fixture) == %{requests: 3, attempts: 2, turns: 2, links: 2, settlements: 2}
      assert Repo.get!(Request, original.id).request_metadata == original.request_metadata
      refute Map.has_key?(original.request_metadata, "mailbox")
      assert Repo.get!(Request, first.id).native_client_retry_digest == witness(fixture, first_payload, unquote(successor_transport)).digest
    end
  end

  for transport <- ["websocket", "http_sse"] do
    @tag slow: "observes two actual one-second lease deadlines through the renewal API"
    test "#{transport} mailbox chain proves every expiry-replaced historical session", %{fixture: fixture} do
      transport = unquote(transport)
      original = admit!(fixture, fixture.payload, transport)
      first_output = reasoning("replacement-first")
      cut!(fixture, original, first_output)
      replacement = replace_expired_fixture!(fixture)
      first_payload = append(fixture.payload, [first_output, mailbox(1)])
      first = admit!(replacement, first_payload, transport)
      assert_edge!(original, first)
      second_output = reasoning("replacement-second")
      cut!(replacement, first, second_output)
      newest = replace_expired_fixture!(replacement)
      candidate = append(first_payload, [second_output, mailbox(2)])

      # The latest edge alone is insufficient: invalidate the oldest genuine
      # expiry reason while retaining the newer edge's certificate.
      Repo.update_all(from(s in CodexSession, where: s.id == ^fixture.session.id), set: [close_reason: nil])
      assert_refused_opts!(newest, options(newest, candidate, transport), :terminal_predecessor)
      assert counts(newest) == %{requests: 2, attempts: 2, turns: 2, links: 1, settlements: 2}
    end
  end

  for transport <- ["websocket", "http_sse"] do
    @tag slow: "observes two actual one-second lease deadlines through the renewal API"
    test "#{transport} mailbox chain crosses two certified expiry replacements", %{fixture: fixture} do
      transport = unquote(transport)
      original = admit!(fixture, fixture.payload, transport)
      first_output = reasoning("replacement-first")
      cut!(fixture, original, first_output)
      replacement = replace_expired_fixture!(fixture)
      first_payload = append(fixture.payload, [first_output, mailbox(1)])
      first = admit!(replacement, first_payload, transport)
      second_output = reasoning("replacement-second")
      cut!(replacement, first, second_output)
      newest = replace_expired_fixture!(replacement)
      second = admit!(newest, append(first_payload, [second_output, mailbox(2)]), transport)
      assert_edge!(original, first)
      assert_edge!(first, second)
      assert counts(newest) == %{requests: 3, attempts: 2, turns: 2, links: 2, settlements: 2}
      assert original.correlation_id == fixture.claim
    end
  end

  for transport <- ["websocket", "http_sse"] do
    @tag mailbox_historical_target: true
    @tag slow: "observes two actual lease deadlines and invalidates only the intermediate creation order"
    test "#{transport} historical authority binds the stored intermediate successor", %{fixture: fixture} do
      transport = unquote(transport)
      original = admit!(fixture, fixture.payload, transport)
      output = reasoning("historical-target-first")
      cut!(fixture, original, output)
      intermediate = replace_expired_fixture!(fixture)
      first_payload = append(fixture.payload, [output, mailbox(1)])
      first = admit!(intermediate, first_payload, transport)
      later_output = reasoning("historical-target-second")
      cut!(intermediate, first, later_output)
      newest = replace_expired_fixture!(intermediate)
      old = Repo.get!(CodexSession, fixture.session.id)
      middle = Repo.get!(CodexSession, intermediate.session.id)
      current = Repo.get!(CodexSession, newest.session.id)
      Repo.update_all(from(s in CodexSession, where: s.id == ^middle.id), set: [created_at: DateTime.add(old.closed_at, -1, :microsecond)])
      assert Repo.get!(CodexSession, middle.id).close_reason == "owner_lease_expired"
      assert Repo.get!(CodexSession, old.id) == old
      assert Repo.get!(CodexSession, current.id) == current
      candidate = append(first_payload, [later_output, mailbox(2)])
      if transport == "http_sse", do: assert_http_refused!(newest, candidate, :terminal_predecessor), else: assert_refused_opts!(newest, options(newest, candidate, transport), :terminal_predecessor)
      assert_edge!(original, first)
    end
  end

  for role <- [:opening, :local_resume], predecessor_transport <- ["websocket", "http_sse"], successor_transport <- ["websocket", "http_sse"] do
    @tag mailbox_general: true
    test "#{role} mailbox cuts chain from #{predecessor_transport} to #{successor_transport} without changing the original claim", %{fixture: fixture} do
      fixture = ordinary_fixture(fixture, unquote(role))
      original = admit!(fixture, fixture.payload, unquote(predecessor_transport))
      output = reasoning("ordinary-first")
      cut!(fixture, original, output)
      first_payload = append(fixture.payload, [output, mailbox(1)])
      first = admit!(fixture, first_payload, unquote(successor_transport))
      assert_edge!(original, first)
      next_output = reasoning("ordinary-second")
      cut!(fixture, first, next_output)
      expire!(original)
      second = admit!(fixture, append(first_payload, [next_output, mailbox(2)]), unquote(successor_transport))
      assert_edge!(first, second)
      assert original.correlation_id == fixture.claim
      assert counts(fixture) == %{requests: 3, attempts: 2, turns: 2, links: 2, settlements: 2}
    end
  end

  for predecessor_transport <- ["websocket", "http_sse"], successor_transport <- ["websocket", "http_sse"] do
    @tag mailbox_delivered: true
    test "a delivered #{predecessor_transport} resume admits one retained prefix with mailbox through #{successor_transport}", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(predecessor_transport))
      first = reasoning("delivered-first")
      second = reasoning("delivered-second")
      served!(fixture, original, [first, second])
      candidate = append(fixture.payload, [first, mailbox(1)])
      assert ClientRetry.verified_mailbox_continuation?(Repo.get_by!(CodexTurn, request_id: original.id), Repo.get!(Request, original.id), Repo.get_by!(Attempt, request_id: original.id), witness(fixture, candidate, unquote(successor_transport)), nil)
      assert_refused!(fixture, append(fixture.payload, [second, mailbox(1)]), :terminal_predecessor)
      assert_http_refused!(fixture, append(fixture.payload, [second, mailbox(1)]), :terminal_predecessor)
      admitted = admit!(fixture, candidate, unquote(successor_transport))
      assert_edge!(original, admitted)
      assert_refused!(fixture, candidate, :active_predecessor)
      assert Repo.get!(Request, original.id).status == "succeeded"
      assert counts(fixture) == %{requests: 2, attempts: 1, turns: 1, links: 1, settlements: 1}
    end
  end

  for transport <- ["websocket", "http_sse"] do
    test "a served #{transport} receipt still requires current authorization, full prefix evidence and the original session", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(transport))
      output = reasoning("served-negative")
      served!(fixture, original, [output])
      candidate = append(fixture.payload, [output, mailbox(1)])
      options = options(fixture, candidate, "websocket")
      witness = %{options.native_client_retry_witness | auth_epoch: options.native_client_retry_witness.auth_epoch + 1}
      assert_refused_opts!(fixture, %{options | native_client_retry_witness: witness}, :terminal_predecessor)
      other = insert_session!(fixture.setup)
      assert_refused_opts!(fixture, %{options | codex_session: other}, :terminal_predecessor)
      assert_refused!(fixture, append(fixture.payload, [mailbox(1)]), :terminal_predecessor)
      call = %{"type" => "function_call", "call_id" => "call_synthetic", "name" => "synthetic_tool", "arguments" => "{}"}
      assert_refused!(fixture, append(fixture.payload, [output, call, mailbox(1)]), :terminal_predecessor)
      attempt = Repo.get_by!(Attempt, request_id: original.id)
      attempt |> Ecto.Changeset.change(response_metadata: %{}) |> Repo.update!()
      assert_refused!(fixture, candidate, :terminal_predecessor)
    end

    test "a served #{transport} mailbox continuation keeps the current predecessor's retry window", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(transport))
      output = reasoning("served-expired")
      served!(fixture, original, [output])
      expire!(original)
      assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :retry_expired)
      assert_http_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :retry_expired)
    end
  end

  test "changed mailbox output stays fenced while settled identical resends chain", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)

    assert_refused!(fixture, append(fixture.payload, [reasoning("changed"), mailbox(1)]), :terminal_predecessor)
    assert_http_refused!(fixture, append(fixture.payload, [reasoning("changed"), mailbox(1)]), :terminal_predecessor)

    candidate = append(fixture.payload, [output, mailbox(1)])
    successor = admit!(fixture, candidate, "websocket")
    assert_edge!(original, successor)
    assert_refused!(fixture, candidate, :active_predecessor)
    cut!(fixture, successor, reasoning("second"))
    repeated = admit!(fixture, candidate, "websocket")
    assert_edge!(successor, repeated)
    assert_refused!(fixture, candidate, :active_predecessor)
    cut!(fixture, repeated, reasoning("third"))
    # This fixture's HTTP resume witness uses a different projection from its
    # websocket witness; the mismatch remains refused.
    assert_http_refused!(fixture, candidate, :terminal_predecessor)
  end

  for transport <- ["websocket", "http_sse"] do
    test "identical interrupted resume chains over #{transport}", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(transport))
      cut!(fixture, original, reasoning("first"))
      successor = admit!(fixture, fixture.payload, unquote(transport))
      assert_edge!(original, successor)
      if unquote(transport) == "websocket", do: assert_refused!(fixture, fixture.payload, :active_predecessor)
    end
  end

  test "a historical mailbox end must equal the successor's actual stored witness", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    first_payload = append(fixture.payload, [output, mailbox(1)])
    first = admit!(fixture, first_payload, "websocket")
    next_output = reasoning("second")
    cut!(fixture, first, next_output)

    altered_history = append(fixture.payload, [output, mailbox(2), next_output, mailbox(3)])
    assert_refused!(fixture, altered_history, :terminal_predecessor)
    assert_edge!(original, first)
  end

  test "an HTTP consumed prefix requires its persisted prefix proof", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "http_sse")
    first = reasoning("first")
    second = reasoning("second")
    cut!(fixture, original, first)
    progress = ClientRetry.new_native_http_progress() |> ClientRetry.observe_native_http_output_item(first) |> ClientRetry.observe_native_http_output_item(second)
    attempt = Repo.get_by!(Attempt, request_id: original.id)
    metadata = Map.put(attempt.response_metadata, "native_http_resume_progress", ClientRetry.native_http_progress_metadata(progress))
    attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
    candidate = append(fixture.payload, [Map.put(first, "content", nil), mailbox(1)])
    assert_http_refused!(fixture, candidate, :terminal_predecessor)
    metadata = Map.put(metadata, "native_http_mailbox_prefix", ClientRetry.native_http_mailbox_prefix_metadata(progress))
    attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
    successor = admit!(fixture, candidate, "http_sse")
    assert_edge!(original, successor)
  end

  for live_row <- [:request, :attempt, :turn] do
    test "a live #{live_row} retains the duplicate fence", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, "websocket")
      output = reasoning("first")
      cut!(fixture, original, output)
      make_live!(original, unquote(live_row))
      assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :active_predecessor)
    end
  end

  test "the current predecessor's expired retry window retains the fence", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    expire!(original)
    assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :retry_expired)
  end

  test "changed authorization epoch and session cannot redeem a mailbox continuation", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    candidate = append(fixture.payload, [output, mailbox(1)])
    opts = options(fixture, candidate, "websocket")
    changed_epoch = %{opts.native_client_retry_witness | auth_epoch: opts.native_client_retry_witness.auth_epoch + 1}
    assert_refused_opts!(fixture, %{opts | native_client_retry_witness: changed_epoch}, :terminal_predecessor)
    other_session = insert_session!(fixture.setup)
    assert_refused_opts!(fixture, %{opts | codex_session: other_session}, :terminal_predecessor)
  end

  test "a foreign successor link retains the fence", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    foreign_fixture = %{fixture | claim: "codex-turn:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)}
    foreign = admit!(foreign_fixture, fixture.payload, "websocket")
    ClientRetry.insert_link!(original, foreign, db_now())
    assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :terminal_predecessor)
  end

  for transport <- ["websocket", "http_sse"], transition <- [:second_window, :replace_successor, :close_original, :expiry_then_window] do
    @tag mailbox_composition: true
    if transition != :second_window, do: @tag(slow: "observes actual lease replacement through the renewal and start APIs")

    test "#{transport} historical authority composes #{transition}", %{fixture: fixture} do
      assert_mailbox_composition(fixture, unquote(transport), unquote(transition))
    end
  end

  for transport <- ["websocket", "http_sse"], mutation <- [:reversed, :skipped, :forged_current, :nonexpiry_close, :successor_session, :successor_witness] do
    test "#{transport} historical mailbox edge refuses #{mutation}", %{fixture: fixture} do
      assert_historical_refusal(fixture, unquote(transport), unquote(mutation))
    end
  end

  test "returning to the ancestor's session cannot bypass its stored successor edge", %{fixture: fixture} do
    thread = Ecto.UUID.generate()
    previous = window_fixture(fixture, "#{thread}:2")
    original = admit!(previous, fixture.payload, "websocket")
    output = reasoning("ancestor-session")
    cut!(previous, original, output)
    successor = window_fixture(previous, "#{thread}:3")
    candidate = append(fixture.payload, [output, mailbox(1)])
    admitted = admit!(successor, candidate, "websocket")
    next_output = reasoning("ancestor-successor")
    cut!(successor, admitted, next_output)
    scope = %{pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, model_id: fixture.setup.model.id, endpoint: @endpoint, native_window_id: "#{thread}:2"}
    witness = witness(previous, append(candidate, [next_output, mailbox(2)]), "websocket")
    turn = Repo.get_by!(CodexTurn, request_id: original.id)
    request = Repo.get!(Request, original.id)
    attempt = Repo.get_by!(Attempt, request_id: original.id)
    admitted = Repo.get!(Request, admitted.id)

    assert {:ok, %{stage: :session}} = SessionContinuity.mailbox_admission_transaction(fn -> [previous.session.id, successor.session.id] end, fn -> ClientRetry.mailbox_check_for_session(turn, request, attempt, witness, admitted, previous.session.id, scope) end, :unexpected_rediscovery)
  end

  defp assert_historical_refusal(fixture, transport, mutation) do
    thread = Ecto.UUID.generate()
    previous = window_fixture(fixture, "#{thread}:2")
    original = admit!(previous, fixture.payload, transport)
    output = reasoning("historical-original")
    cut!(previous, original, output)
    middle = window_fixture(previous, "#{thread}:3")
    first_payload = append(fixture.payload, [output, mailbox(1)])
    first = admit!(middle, first_payload, transport)
    next_output = reasoning("historical-next")
    cut!(middle, first, next_output)
    current = window_fixture(middle, "#{thread}:4")
    current = invalidate_history!(mutation, previous, middle, current, first, thread)
    candidate = append(first_payload, [next_output, mailbox(2)])
    if transport == "websocket", do: assert_refused!(current, candidate, :terminal_predecessor), else: assert_http_refused!(current, candidate, :terminal_predecessor)
    assert counts(current) == %{requests: 2, attempts: 2, turns: 2, links: 1, settlements: 2}
  end

  defp invalidate_history!(:reversed, previous, _middle, current, _first, thread) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^previous.session.id), set: [session_key: SessionContinuity.window_session_key("#{thread}:5")])
    current
  end

  defp invalidate_history!(:skipped, previous, _middle, current, _first, thread) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^previous.session.id), set: [session_key: SessionContinuity.window_session_key("#{thread}:1")])
    current
  end

  defp invalidate_history!(:forged_current, _previous, _middle, current, _first, _thread), do: %{current | native_window_id: "#{Ecto.UUID.generate()}:4"}

  defp invalidate_history!(:nonexpiry_close, previous, _middle, current, _first, _thread) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^previous.session.id), set: [status: "closed", closed_at: db_now()])
    current
  end

  defp invalidate_history!(:successor_session, _previous, middle, current, first, _thread) do
    other = insert_session!(middle.setup)
    Repo.update_all(from(t in CodexTurn, where: t.request_id == ^first.id), set: [codex_session_id: other.id])
    current
  end

  defp invalidate_history!(:successor_witness, _previous, _middle, current, first, _thread) do
    Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [native_client_retry_digest: :crypto.strong_rand_bytes(32)])
    current
  end

  defp assert_mailbox_composition(fixture, transport, transition) do
    thread = Ecto.UUID.generate()
    original_fixture = window_fixture(fixture, "#{thread}:2")
    original = admit!(original_fixture, fixture.payload, transport)
    first_output = reasoning("history-first")
    cut!(original_fixture, original, first_output)

    middle =
      if transition == :expiry_then_window,
        do: replace_expired_fixture!(original_fixture),
        else: window_fixture(original_fixture, "#{thread}:3")

    first_payload = append(fixture.payload, [first_output, mailbox(1)])
    first = admit!(middle, first_payload, transport)
    assert_edge!(original, first)
    second_output = reasoning("history-second")
    cut!(middle, first, second_output)

    current =
      case transition do
        :second_window ->
          window_fixture(middle, "#{thread}:4")

        :replace_successor ->
          replace_expired_fixture!(middle)

        :close_original ->
          replace_expired_fixture!(original_fixture)
          middle

        :expiry_then_window ->
          window_fixture(middle, "#{thread}:3")
      end

    candidate = append(first_payload, [second_output, mailbox(2)])
    second = admit!(current, candidate, transport)
    assert_edge!(first, second)
  end

  defp assert_window_successor(fixture, predecessor_transport, successor_transport) do
    thread = Ecto.UUID.generate()
    fixture = window_fixture(fixture, "#{thread}:2")
    original = admit!(fixture, fixture.payload, predecessor_transport)
    output = reasoning("window-cut")
    cut!(fixture, original, output)
    Repo.update_all(from(s in CodexSession, where: s.id == ^fixture.session.id), set: [status: "interrupted"])
    previous = Repo.get!(CodexSession, fixture.session.id)
    assert previous.owner_lease_token
    assert DateTime.compare(previous.owner_lease_expires_at, db_now()) == :gt
    assert is_nil(previous.close_reason)
    successor = window_fixture(fixture, "#{thread}:3")
    refute successor.session.id == previous.id
    candidate = append(fixture.payload, [output, mailbox(1)])
    changed = append(fixture.payload, [reasoning("changed-window-output"), mailbox(1)])
    assert_refused!(successor, changed, :terminal_predecessor)
    assert_http_refused!(successor, changed, :terminal_predecessor)
    admitted = admit!(successor, candidate, successor_transport)
    assert_edge!(original, admitted)
    assert counts(fixture) == %{requests: 2, attempts: 1, turns: 1, links: 1, settlements: 1}
    assert Repo.get!(CodexSession, previous.id) == previous
    assert_refused!(successor, candidate, :terminal_predecessor)

    # A later cut in the new session still verifies the original cross-window edge.
    next_output = reasoning("next-window-cut")
    cut!(successor, admitted, next_output)
    next = admit!(successor, append(candidate, [next_output, mailbox(2)]), successor_transport)
    assert_edge!(admitted, next)
  end

  defp window_fixture(fixture, window) do
    opts = RequestOptions.for_websocket(%{session_header: window, session_header_source: "x-codex-window-id"})
    assert {:ok, session} = SessionContinuity.start_codex_session(fixture.setup.auth, opts)
    Map.merge(fixture, %{session: session, native_window_id: window})
  end

  defp admit!(fixture, payload, "websocket") do
    opts = options(fixture, payload, "websocket")
    assert {:ok, %{request: claim}} = Accounting.claim_websocket_turn(fixture.setup.auth, fixture.setup.model, opts)
    assert {:ok, %{request: request}} = Accounting.reserve(fixture.setup.auth, fixture.setup.model, payload, Map.put(opts, :turn_claim, claim))
    request
  end

  defp admit!(fixture, payload, "http_sse") do
    assert {:ok, %{request: request}} = Accounting.reserve(fixture.setup.auth, fixture.setup.model, payload, options(fixture, payload, "http_sse"))
    request
  end

  defp options(fixture, payload, transport) do
    metadata = if transport == "http_sse", do: %{"native_http_claim_arm" => Map.get(fixture, :arm, "post_compaction_resume"), "native_http_input_count" => length(payload["input"])}, else: %{}

    %{
      endpoint: @endpoint,
      transport: transport,
      correlation_id: fixture.claim,
      codex_session: fixture.session,
      native_window_id: Map.get(fixture, :native_window_id),
      requested_model: fixture.setup.model.exposed_model_id,
      native_client_retry_witness: witness(fixture, payload, transport),
      native_http_input_count: length(payload["input"]),
      native_http_semantic_turn_key: fixture.semantic,
      request_metadata: metadata,
      reservation_estimate: %{input_tokens: 10, output_tokens: 10}
    }
  end

  defp witness(fixture, payload, transport) do
    {:ok, digest} =
      case transport do
        "websocket" -> WebsocketTurnIdentity.replay_claim_digest(fixture.semantic, payload)
        "http_sse" -> if Map.get(fixture, :arm) == "opening", do: WebsocketTurnIdentity.replay_claim_digest(fixture.semantic, payload), else: WebsocketTurnIdentity.http_resume_input_digest(fixture.semantic, payload["input"])
      end

    ClientRetry.original_witness!(digest, fixture.setup.api_key.runtime_revocation_epoch)
    |> NativeMailboxContinuation.attach(fixture.semantic, payload, RequestOptions.build(%{}, @endpoint, %{}))
  end

  defp cut!(fixture, request, output) do
    now = db_now()
    {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(output)
    receipt = %{"outcome" => "aborted", "terminal_class" => "none", "highest_frame_class" => "item_done", "completed_items" => 1, "completed_item_digests" => [digest]}
    progress = ClientRetry.new_native_http_progress() |> ClientRetry.observe_native_http_output_item(output) |> ClientRetry.native_http_progress_metadata()
    assert {:ok, attempt} = Accounting.create_attempt(request, fixture.setup.assignment, %{transport: request.transport})
    assert {:ok, _finalized} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499, usage: %{status: "usage_unknown", source: "client_disconnected"}, attempt_metadata: %{"downstream_delivery" => receipt, "native_http_resume_progress" => progress}})
    sequence = Repo.one(from turn in CodexTurn, where: turn.codex_session_id == ^fixture.session.id, select: coalesce(max(turn.turn_sequence), 0)) + 1

    Repo.insert!(%CodexTurn{
      codex_session_id: fixture.session.id,
      request_id: request.id,
      turn_sequence: sequence,
      transport_kind: request.transport,
      semantic_turn_digest: fixture.semantic,
      status: "interrupted",
      error_code: "client_disconnected",
      final_attempt_id: attempt.id,
      first_visible_output_at: now,
      started_at: now,
      completed_at: now,
      created_at: now,
      updated_at: now
    })
  end

  defp ordinary_fixture(fixture, role) do
    input = [hd(fixture.payload["input"])]
    input = if role == :local_resume, do: input ++ [%{"type" => "message", "role" => "user", "content" => "synthetic local summary"}], else: input
    payload = Map.put(fixture.payload, "input", input)
    claim = if role == :local_resume, do: WebsocketTurnIdentity.steered_claim_key(fixture.semantic, NativeTurnContinuation.turn_progress(payload, 2)), else: "codex-turn:" <> Base.url_encode64(fixture.semantic, padding: false)
    Map.merge(fixture, %{payload: payload, claim: claim, arm: "opening"})
  end

  defp served!(fixture, request, outputs) do
    now = db_now()

    digests =
      Enum.map(outputs, fn output ->
        {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(output)
        digest
      end)

    receipt = %{"outcome" => "delivered", "terminal_class" => "response.completed", "highest_frame_class" => "terminal", "completed_items" => length(outputs), "completed_item_digests" => digests}
    progress = Enum.reduce(outputs, ClientRetry.new_native_http_progress(), &ClientRetry.observe_native_http_output_item(&2, &1))
    metadata = %{"downstream_delivery" => receipt, "native_http_resume_progress" => ClientRetry.native_http_progress_metadata(progress), "native_http_mailbox_prefix" => ClientRetry.native_http_mailbox_prefix_metadata(progress)}
    assert {:ok, attempt} = Accounting.create_attempt(request, fixture.setup.assignment, %{transport: request.transport})
    assert {:ok, _} = Accounting.finalize_success(request, attempt, %{status: "measured", source: "synthetic", input_tokens: 10, output_tokens: 2}, %{attempt_metadata: metadata})
    Repo.insert!(%CodexTurn{codex_session_id: fixture.session.id, request_id: request.id, turn_sequence: 1, transport_kind: request.transport, semantic_turn_digest: fixture.semantic, status: "succeeded", final_attempt_id: attempt.id, first_visible_output_at: now, started_at: now, completed_at: now, created_at: now, updated_at: now})
  end

  defp assert_refused!(fixture, payload, disposition), do: assert_refused_opts!(fixture, options(fixture, payload, "websocket"), disposition)

  defp assert_refused_opts!(fixture, opts, disposition) do
    before = counts(fixture)
    assert {:error, %{code: :duplicate_request, resend_disposition: ^disposition}} = Accounting.claim_websocket_turn(fixture.setup.auth, fixture.setup.model, opts)
    assert counts(fixture) == before
  end

  defp assert_http_refused!(fixture, payload, disposition) do
    before = counts(fixture)
    assert {:error, %{code: :duplicate_request, resend_disposition: ^disposition}} = Accounting.reserve(fixture.setup.auth, fixture.setup.model, payload, options(fixture, payload, "http_sse"))
    assert counts(fixture) == before
  end

  defp assert_edge!(predecessor, successor) do
    assert {:ok, successor.correlation_id} == ClientRetry.deterministic_failed_predecessor_claim(predecessor.correlation_id, predecessor.id)
    assert successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id
    assert Repo.exists?(from link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor.id and link.successor_request_id == ^successor.id)
  end

  defp counts(fixture) do
    requests = from request in Request, where: request.pool_id == ^fixture.setup.pool.id, select: request.id

    %{
      requests: Repo.aggregate(requests, :count),
      attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in subquery(requests)), :count),
      turns: Repo.aggregate(from(t in CodexTurn, where: t.request_id in subquery(requests)), :count),
      links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in subquery(requests)), :count),
      settlements: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in subquery(requests) and l.entry_kind == "settlement"), :count)
    }
  end

  defp make_live!(request, :request), do: Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [status: "in_progress", completed_at: nil])
  defp make_live!(request, :attempt), do: Repo.update_all(from(a in Attempt, where: a.request_id == ^request.id), set: [status: "in_progress", completed_at: nil])
  defp make_live!(request, :turn), do: Repo.update_all(from(t in CodexTurn, where: t.request_id == ^request.id), set: [status: "in_progress", completed_at: nil])

  defp expire!(request) do
    expired = DateTime.add(db_now(), -31, :second)
    Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [completed_at: expired])
    Repo.update_all(from(a in Attempt, where: a.request_id == ^request.id), set: [completed_at: expired])
    Repo.update_all(from(t in CodexTurn, where: t.request_id == ^request.id), set: [completed_at: expired])
  end

  defp replace_expired_fixture!(fixture) do
    opts = RequestOptions.for_websocket(%{session_key: fixture.session.session_key})
    assert {:ok, owned} = SessionContinuity.start_codex_session(fixture.setup.auth, opts)
    assert owned.id == fixture.session.id
    assert {:ok, renewed} = SessionContinuity.renew_owner_token(owned, owned.owner_lease_token, RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: 1}))
    await_fixture_expiry!(renewed.owner_lease_expires_at, System.monotonic_time(:millisecond) + 15_000)
    assert {:ok, replacement} = SessionContinuity.start_codex_session(fixture.setup.auth, opts)
    assert replacement.id != owned.id
    closed = Repo.get!(CodexSession, owned.id)
    assert closed.close_reason == "owner_lease_expired"
    assert DateTime.compare(replacement.created_at, closed.closed_at) != :lt
    %{fixture | session: replacement}
  end

  defp await_fixture_expiry!(expires_at, deadline) do
    if DateTime.compare(db_now(), expires_at) == :lt do
      assert System.monotonic_time(:millisecond) < deadline, "owned fixture lease did not expire"
      Process.sleep(20)
      await_fixture_expiry!(expires_at, deadline)
    end
  end

  defp insert_session!(setup) do
    now = db_now()
    Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: "mailbox-chain-#{System.unique_integer([:positive, :monotonic])}", pool_upstream_assignment_id: setup.assignment.id, status: "active", created_at: now, updated_at: now})
  end

  defp claim_for(fixture, payload) do
    {:post_compaction_resume, anchor} = NativeTurnContinuation.turn_role(payload)
    WebsocketTurnIdentity.resume_claim_key(fixture.semantic, anchor)
  end

  defp payload(model), do: %{"type" => "response.create", "model" => model, "stream" => true, "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}, %{"type" => "compaction", "encrypted_content" => "synthetic-pivot"}], "client_metadata" => %{"x-codex-turn-metadata" => %{"turn_id" => "synthetic-turn", "request_kind" => "turn", "agent_name" => "/root"}}}
  defp append(payload, items), do: Map.update!(payload, "input", &(&1 ++ items))
  defp reasoning(id), do: %{"type" => "reasoning", "id" => "rs_" <> id, "summary" => [], "encrypted_content" => "synthetic-reasoning-" <> id}
  defp mailbox(id), do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update #{id}"}]}

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    DateTime.truncate(now, :microsecond)
  end
end
