defmodule CodexPooler.Gateway.Persistence.SessionContinuity do
  @moduledoc false

  import Ecto.Query

  require Logger

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.RequestOptions

  alias CodexPooler.Gateway.Persistence.{
    BridgeOwnerLease,
    CodexSession,
    CodexTurn,
    SessionContinuity.Aliases,
    SessionContinuity.AssignmentClaim,
    SessionContinuity.ExpiredSessions,
    SessionContinuity.OwnerLease,
    SessionContinuity.OwnerWitness,
    SessionContinuity.TurnLifecycle
  }

  alias CodexPooler.Gateway.Persistence.SessionContinuity.MailboxAdmissionLocks
  alias CodexPooler.Gateway.Persistence.SessionContinuity.MailboxSessionAuthority
  alias CodexPooler.Gateway.Persistence.StatusVocabulary.OwnerLease, as: OwnerLeaseStatus
  alias CodexPooler.Gateway.Persistence.StatusVocabulary.Session, as: SessionStatus
  alias CodexPooler.Repo

  @session_active SessionStatus.active_status()
  @session_reconnectable_statuses SessionStatus.reconnectable_statuses()
  @owner_lease_active OwnerLeaseStatus.active_status()
  @continuity_deadlock_retries 1
  @type auth :: CodexPooler.Access.auth_context()
  @type opts :: RequestOptions.t()
  @type payload :: map()
  @type session_result :: {:ok, CodexSession.t()} | {:error, term()}
  @type turn_result :: {:ok, CodexTurn.t()} | {:error, term()}
  @type complete_turn_result ::
          {:ok, %{required(:request) => Request.t(), optional(:attempt) => Attempt.t() | nil}}
          | term()
  @type request_ref :: Request.t() | Ecto.UUID.t()
  @type owner_token_result :: :ok | {:error, :stale_owner | :owner_unavailable}
  @type session_ref :: CodexSession.t() | Ecto.UUID.t() | String.t()

  @doc false
  @spec mailbox_admission_transaction(MailboxAdmissionLocks.discovery(), MailboxAdmissionLocks.operation(), term()) :: {:ok, term()} | {:error, term()}
  defdelegate mailbox_admission_transaction(discover, operation, exhausted_reason), to: MailboxAdmissionLocks, as: :transaction

  @doc false
  @spec require_mailbox_session!(Ecto.UUID.t() | nil) :: :ok
  defdelegate require_mailbox_session!(session_id), to: MailboxAdmissionLocks, as: :require_session!

  @doc false
  @spec mailbox_session_verdict(Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, MailboxSessionAuthority.scope()) :: MailboxSessionAuthority.verdict()
  defdelegate mailbox_session_verdict(previous_id, current_id, scope), to: MailboxSessionAuthority, as: :verdict

  @doc false
  @spec mailbox_session_edge_verdict(Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, MailboxSessionAuthority.scope()) :: MailboxSessionAuthority.verdict()
  defdelegate mailbox_session_edge_verdict(previous_id, successor_id, current_id, scope), to: MailboxSessionAuthority, as: :edge_verdict

  @session_start_conflict_error %{
    status: 409,
    code: "session_start_conflict",
    message: "Session start conflict",
    param: "session_id"
  }

  @spec start_codex_session(auth(), opts()) :: session_result()
  def start_codex_session(auth, %RequestOptions{} = opts) do
    now = now()
    session_key = session_key(opts)
    owner = OwnerLease.owner_instance(opts)

    with :ok <- authorize_runtime_session(auth, opts) do
      Repo.transaction(fn ->
        session = upsert_session_for_start!(auth, opts, session_key, owner, now)
        lease = OwnerLease.acquire!(session, auth, opts, owner, now)
        now = db_now()
        Aliases.register!(session, auth, opts, now)
        OwnerLease.persist_session!(session, lease, now)
      end)
      |> unwrap_transaction()
    end
  end

  @doc false
  @spec validate_session_owner_witness_for_reservation(RequestOptions.t()) :: :ok
  def validate_session_owner_witness_for_reservation(%RequestOptions{
        continuity: %{codex_session: %CodexSession{id: session_id}},
        runtime: %{
          session_owner_witness: %OwnerWitness{session_id: session_id, lease_token: lease_token}
        }
      }) do
    if MailboxAdmissionLocks.coordinated?(), do: MailboxAdmissionLocks.require_session!(session_id)

    case codex_session_for_update(session_id) do
      %CodexSession{} = session ->
        _lease_and_now = lock_and_validate_owner!(session, lease_token)
        :ok

      nil ->
        Repo.rollback(:owner_unavailable)
    end
  end

  def validate_session_owner_witness_for_reservation(%RequestOptions{
        runtime: %{session_owner_witness: %OwnerWitness{}}
      }),
      do: Repo.rollback(:stale_owner)

  def validate_session_owner_witness_for_reservation(%RequestOptions{}), do: :ok

  @spec previous_response_session_id(auth(), String.t()) :: Ecto.UUID.t() | nil
  def previous_response_session_id(auth, previous_response_id), do: Aliases.previous_response_session_id(auth, previous_response_id, db_now())

  @spec previous_response_resolution(auth(), String.t()) :: %{assignment_id: Ecto.UUID.t() | nil, serving_mode: String.t() | nil} | nil
  def previous_response_resolution(auth, previous_response_id), do: Aliases.previous_response_resolution(auth, previous_response_id, db_now())

  @spec previous_response_session_id(auth(), String.t(), DateTime.t()) :: Ecto.UUID.t() | nil
  defdelegate previous_response_session_id(auth, previous_response_id, now), to: Aliases

  @spec previous_response_assignment_id(auth(), String.t(), DateTime.t()) ::
          Ecto.UUID.t() | nil
  defdelegate previous_response_assignment_id(auth, previous_response_id, now), to: Aliases

  @spec previous_response_resolution(auth(), String.t(), DateTime.t()) ::
          %{assignment_id: Ecto.UUID.t() | nil, serving_mode: String.t() | nil} | nil
  defdelegate previous_response_resolution(auth, previous_response_id, now), to: Aliases

  @spec start_codex_session_from_previous_response_id(auth(), opts()) ::
          session_result() | {:error, :session_not_found}
  def start_codex_session_from_previous_response_id(auth, %RequestOptions{} = opts) do
    case blank_to_nil(opts.continuity.previous_response_id) do
      nil ->
        {:error, :session_not_found}

      previous_response_id ->
        start_codex_session_from_previous_response_id(auth, opts, previous_response_id)
    end
  end

  @spec start_codex_session_from_turn_state(auth(), opts()) ::
          session_result() | {:error, :session_not_found}
  def start_codex_session_from_turn_state(auth, %RequestOptions{} = opts) do
    case blank_to_nil(opts.continuity.accepted_turn_state) do
      nil ->
        {:error, :session_not_found}

      turn_state ->
        start_codex_session_from_turn_state(auth, opts, turn_state)
    end
  end

  defp start_codex_session_from_previous_response_id(auth, opts, previous_response_id) do
    now = now()
    owner = OwnerLease.owner_instance(opts)

    with :ok <- authorize_runtime_session(auth, opts) do
      Repo.transaction(fn ->
        auth
        |> previous_response_session_for_update(previous_response_id, now)
        |> start_previous_response_session!(auth, opts, owner, now)
      end)
      |> unwrap_transaction()
    end
  end

  defp start_codex_session_from_turn_state(auth, opts, turn_state) do
    now = now()
    owner = OwnerLease.owner_instance(opts)

    with :ok <- authorize_runtime_session(auth, opts) do
      Repo.transaction(fn ->
        auth.pool.id
        |> Aliases.active_session_for_update(auth.api_key.id, "turn_state", turn_state, now)
        |> start_previous_response_session!(auth, opts, owner, now)
      end)
      |> unwrap_transaction()
    end
  end

  defp previous_response_session_for_update(auth, previous_response_id, now) do
    Aliases.active_session_for_update(
      auth.pool.id,
      auth.api_key.id,
      "previous_response_id",
      previous_response_id,
      now
    )
  end

  defp authorize_runtime_session(auth, %RequestOptions{} = opts) do
    captured_epoch =
      opts.runtime.api_key_runtime_epoch || auth.api_key.runtime_revocation_epoch

    Repo.transaction(fn ->
      case Access.authorize_api_key_runtime_turn_for_read(auth.api_key, captured_epoch) do
        {:ok, _authorization} -> :ok
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_previous_response_session!(%CodexSession{} = session, auth, opts, owner, now) do
    session = update_existing_session!(session, auth, opts, owner, now)
    lease = OwnerLease.acquire!(session, auth, opts, owner, now)
    now = db_now()
    Aliases.register!(session, auth, opts, now)
    OwnerLease.persist_session!(session, lease, now)
  end

  defp start_previous_response_session!(nil, _auth, _opts, _owner, _now),
    do: Repo.rollback(:session_not_found)

  @spec register_codex_session_continuity(
          CodexSession.t(),
          payload(),
          map() | binary(),
          opts()
        ) :: :ok | {:error, term()}

  def register_codex_session_continuity(
        %CodexSession{} = session,
        payload,
        response_body,
        %RequestOptions{} = opts
      )
      when is_map(payload) do
    register_codex_session_continuity(
      session,
      payload,
      response_body,
      opts,
      @continuity_deadlock_retries
    )
  end

  def register_codex_session_continuity(_session, _payload, _response_body, _opts),
    do: {:error, :invalid_session_continuity}

  defp register_codex_session_continuity(session, payload, response_body, opts, retries_left) do
    Repo.transaction(fn ->
      {session, lease, now} = lock_continuity_owner!(session, opts)
      auth = %{pool: %{id: session.pool_id}, api_key: %{id: session.api_key_id}}
      session = maybe_bind_session_assignment!(session, opts, now)

      continuity_opts = Aliases.continuity_opts(opts, payload, response_body)

      Aliases.register!(session, auth, continuity_opts, now)
      renew_continuity_owner!(session, lease, opts, now)
      :ok
    end)
    |> unwrap_ok_transaction()
  rescue
    error in Postgrex.Error ->
      cond do
        deadlock?(error) and retries_left > 0 ->
          register_codex_session_continuity(
            session,
            payload,
            response_body,
            opts,
            retries_left - 1
          )

        deadlock?(error) ->
          {:error, :continuity_deadlock}

        true ->
          reraise error, __STACKTRACE__
      end
  end

  defp deadlock?(%Postgrex.Error{postgres: %{code: :deadlock_detected}}), do: true
  defp deadlock?(%Postgrex.Error{}), do: false

  defp lock_continuity_owner!(
         %CodexSession{id: session_id},
         %RequestOptions{
           runtime: %{
             session_owner_witness: %OwnerWitness{
               session_id: session_id,
               lease_token: lease_token
             }
           }
         }
       ) do
    case codex_session_for_update(session_id) do
      %CodexSession{} = session ->
        {lease, now} = lock_and_validate_owner!(session, lease_token)
        {session, lease, now}

      nil ->
        Repo.rollback(:owner_unavailable)
    end
  end

  defp lock_continuity_owner!(%CodexSession{}, %RequestOptions{
         runtime: %{session_owner_witness: %OwnerWitness{}}
       }) do
    Repo.rollback(:stale_owner)
  end

  defp lock_continuity_owner!(%CodexSession{id: session_id}, %RequestOptions{}) do
    case codex_session_for_update(session_id) do
      %CodexSession{} = session -> {session, nil, now()}
      nil -> Repo.rollback(:owner_unavailable)
    end
  end

  defp renew_continuity_owner!(
         session,
         %CodexPooler.Gateway.Persistence.BridgeOwnerLease{} = lease,
         opts,
         now
       ),
       do: renew_validated_owner!(session, lease, opts, now)

  defp renew_continuity_owner!(session, nil, opts, _now),
    do: OwnerLease.renew_locked!(session, opts)

  defp lock_and_validate_owner!(%CodexSession{} = session, lease_token) do
    case active_owner_lease_for_update(session.id) do
      %BridgeOwnerLease{} = lease ->
        now = db_now()

        cond do
          session.status not in @session_reconnectable_statuses ->
            Repo.rollback(:owner_unavailable)

          expired_at?(session.owner_lease_expires_at, now) or expired_at?(lease.expires_at, now) ->
            Repo.rollback(:owner_unavailable)

          session.owner_lease_token != lease_token or lease.lease_token != lease_token ->
            Repo.rollback(:stale_owner)

          true ->
            {lease, now}
        end

      nil ->
        Repo.rollback(:owner_unavailable)
    end
  end

  defp renew_validated_owner!(session, lease, opts, now) do
    case OwnerLease.validate_renewal_presence(lease, now) do
      :ok -> :ok
      {:error, reason} -> Repo.rollback(reason)
    end

    expires_at = DateTime.add(now, bridge_owner_lease_ttl_seconds(opts), :second)

    renewed_lease =
      lease
      |> Ecto.Changeset.change(%{
        pool_upstream_assignment_id: session.pool_upstream_assignment_id,
        renewed_at: now,
        expires_at: expires_at,
        updated_at: now
      })
      |> Repo.update!()

    OwnerLease.persist_session!(session, renewed_lease, now)
  end

  defp active_owner_lease_for_update(session_id) do
    Repo.one(
      from lease in BridgeOwnerLease,
        where: lease.codex_session_id == ^session_id and lease.status == ^@owner_lease_active,
        order_by: [desc: lease.renewed_at, desc: lease.created_at],
        limit: 1,
        lock: "FOR UPDATE"
    )
  end

  defp expired_at?(%DateTime{} = expires_at, now), do: DateTime.compare(expires_at, now) != :gt
  defp expired_at?(_expires_at, _now), do: true

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    now
  end

  @spec validate_owner_token(session_ref(), Ecto.UUID.t() | String.t()) :: owner_token_result()
  defdelegate validate_owner_token(session_ref, owner_lease_token), to: OwnerLease, as: :validate

  @spec renew_owner_token(session_ref(), Ecto.UUID.t() | String.t(), opts()) ::
          {:ok, CodexSession.t()} | {:error, :stale_owner | :owner_unavailable}
  defdelegate renew_owner_token(session_ref, owner_lease_token, opts), to: OwnerLease

  @spec renew_owner_token(
          session_ref(),
          Ecto.UUID.t() | String.t(),
          opts(),
          [OwnerLease.renewal_option()]
        ) ::
          {:ok, CodexSession.t()}
          | {:error,
             :stale_owner
             | :owner_unavailable
             | {:lock_timeout, __MODULE__.LockWaitDiagnostics.t()}}
  defdelegate renew_owner_token(session_ref, owner_lease_token, opts, renewal_opts),
    to: OwnerLease

  @doc """
  Pins a session that has no pin to the account serving its client output,
  under the request's own owner lease token (findings#324).
  """
  @spec claim_session_assignment(Ecto.UUID.t(), Ecto.UUID.t(), Ecto.UUID.t()) :: AssignmentClaim.result()
  defdelegate claim_session_assignment(session_id, assignment_id, lease_token), to: AssignmentClaim, as: :claim

  @spec start_codex_turn(CodexSession.t(), Request.t(), opts()) :: turn_result()
  defdelegate start_codex_turn(session, request, opts), to: TurnLifecycle
  @spec lock_codex_session_for_turn(CodexSession.t()) :: CodexSession.t()
  def lock_codex_session_for_turn(%CodexSession{id: session_id} = session) do
    if MailboxAdmissionLocks.coordinated?(), do: MailboxAdmissionLocks.require_session!(session_id)
    TurnLifecycle.lock_codex_session_for_turn(session)
  end

  @spec complete_codex_turn(complete_turn_result(), String.t(), term()) :: term()
  defdelegate complete_codex_turn(result, status, error_code), to: TurnLifecycle

  @spec complete_codex_turn(
          complete_turn_result(),
          String.t(),
          term(),
          CodexPooler.Accounting.Attempt.t()
        ) ::
          term()
  defdelegate complete_codex_turn(result, status, error_code, attempt), to: TurnLifecycle

  @spec complete_codex_turn(
          complete_turn_result(),
          String.t(),
          term(),
          CodexPooler.Accounting.Attempt.t(),
          OwnerWitness.t() | nil
        ) :: term()
  defdelegate complete_codex_turn(result, status, error_code, attempt, owner_witness),
    to: TurnLifecycle

  @spec mark_codex_turn_visible(request_ref()) :: :ok
  defdelegate mark_codex_turn_visible(request_ref), to: TurnLifecycle

  @spec mark_codex_turn_visible(request_ref(), CodexPooler.Accounting.Attempt.t()) ::
          :ok | {:error, :stale_generation}
  defdelegate mark_codex_turn_visible(request_ref, attempt), to: TurnLifecycle

  @doc false
  @spec authorize_codex_turn_visibility(request_ref(), map()) ::
          {:ok, TurnLifecycle.visibility_witness()} | {:error, :stale_generation}
  defdelegate authorize_codex_turn_visibility(request_ref, attempt), to: TurnLifecycle

  @spec release_owner_lease(session_ref(), Ecto.UUID.t() | String.t(), String.t()) ::
          :ok | {:error, :stale_owner | :owner_unavailable}
  defdelegate release_owner_lease(session_ref, owner_lease_token, reason),
    to: OwnerLease,
    as: :release

  @spec release_owner_lease(
          session_ref(),
          Ecto.UUID.t() | String.t(),
          String.t(),
          :idle_expiry | :drain_cut | nil
        ) :: :ok | {:error, :stale_owner | :owner_unavailable}
  defdelegate release_owner_lease(session_ref, owner_lease_token, reason, owner_exit_cause),
    to: OwnerLease,
    as: :release

  @spec replace_unavailable_owner_lease(session_ref(), opts()) :: session_result()
  defdelegate replace_unavailable_owner_lease(session_ref, opts),
    to: OwnerLease,
    as: :replace_unavailable

  defp codex_session_for_update(session_id) do
    Repo.one(codex_session_for_update_query(session_id))
  end

  defp codex_session_for_update_query(session_id) do
    from session in CodexSession,
      where: session.id == ^session_id,
      lock: "FOR UPDATE"
  end

  defp upsert_session_for_start!(auth, opts, session_key, owner, now) do
    case existing_session_for_start!(auth, opts, session_key, now) do
      {%CodexSession{} = session, _preference} ->
        update_existing_session!(session, auth, opts, owner, now)

      {nil, preference} ->
        maybe_test_block_before_session_insert()
        insert_new_session!(auth, opts, session_key, owner, now, preference)
    end
  end

  # Returns the session to reuse, if any, together with the assignment a new
  # session should softly prefer when there is nothing to reuse. The
  # recreation preference is produced by the same transaction and row locks
  # that close the lease-expired sessions, so the assignment cannot change
  # underneath the insert that follows. The lease-expired sessions closed are
  # the ones of this key the request reaches by its session key or through a
  # window alias (findings#270 row 270-282), since a window linked to another
  # window's session is keyed by that other window.
  defp existing_session_for_start!(auth, opts, session_key, now) do
    resolved_session = Aliases.resolved_session_for_update(auth, opts, session_key, now)

    preferred_assignment_id =
      if is_nil(resolved_session) do
        ExpiredSessions.close_for_key_and_aliases!(
          auth.pool.id,
          auth.api_key.id,
          session_key,
          Aliases.session_header_values(opts),
          now
        ).preferred_assignment_id
      end

    existing_session =
      resolved_session || active_session_for_update(auth, session_key, now) ||
        previous_window_session_for_update(auth, opts, now)

    if is_nil(existing_session) and authenticated_owner_attach_requires_existing?(opts) do
      Repo.rollback(:owner_unavailable)
    end

    {existing_session, session_preference(existing_session, preferred_assignment_id, auth, opts, now)}
  end

  # A new session prefers the assignment of the lease-expired session of its
  # key it replaces (row 270-282); failing that, a native websocket upgrade
  # prefers the assignment of its thread's previous window's live session, on
  # a window no session knew yet (row 270-283). The upgrade does not join that
  # session as a native HTTP request does (findings#289): an owner serves the
  # socket that attached last, and a second live process on the thread (a
  # stale resumed process reconnecting on the old window) would displace the
  # socket that joined, whose next turn then meets `stale_owner`.
  defp session_preference(%CodexSession{}, _assignment_id, _auth, _opts, _now), do: nil
  defp session_preference(nil, assignment_id, _auth, _opts, _now) when is_binary(assignment_id), do: {:recreated, assignment_id}
  defp session_preference(nil, _assignment_id, auth, opts, now), do: previous_window_preference(auth, opts, now)

  defp previous_window_preference(auth, opts, now) do
    case Aliases.previous_window_preference_session(auth, opts, now) do
      %CodexSession{pool_upstream_assignment_id: assignment_id} = session when is_binary(assignment_id) ->
        Logger.info("websocket upgrade window preference previous_codex_session_id=#{session.id} alias_preview=#{window_alias_preview(opts)} disposition=preferred")
        {:previous_window, assignment_id}

      %CodexSession{} = session ->
        Logger.info("websocket upgrade window preference previous_codex_session_id=#{session.id} alias_preview=#{window_alias_preview(opts)} disposition=unassigned")
        nil

      nil ->
        nil
    end
  end

  # A native HTTP request whose own window has no live session continues the
  # live session of its thread's previous window (findings#289). The released
  # Codex client names the next window on the request that resumes after a
  # compaction and nothing else changes; on the websocket the socket's session
  # carries the thread across (P115), over HTTP only the previous window's
  # alias does. `Aliases.register!/4` then registers the request's window on
  # that session, as it registers any window of a reused session.
  defp previous_window_session_for_update(auth, opts, now) do
    case Aliases.previous_window_session_for_update(auth, opts, now) do
      %CodexSession{} = session ->
        Logger.info("http window alias codex_session_id=#{session.id} alias_preview=#{window_alias_preview(opts)} disposition=linked")
        session

      nil ->
        nil
    end
  end

  defp window_alias_preview(%RequestOptions{continuity: %{session_header: window}}) do
    :crypto.hash(:sha256, String.trim(window))
    |> Base.encode16(case: :lower)
    |> String.slice(0, 16)
  end

  # Every lookup that reaches here is scoped to the requesting API key, so the
  # row already belongs to it; the key is never rewritten, which is what let a
  # second key of the Pool re-own another key's session (findings#255).
  defp update_existing_session!(%CodexSession{} = session, _auth, opts, owner, now) do
    session
    |> Ecto.Changeset.change(%{
      status: @session_active,
      owner_instance_id: owner.node_name,
      owner_instance_boot_id: owner.boot_id,
      owner_lease_token: session.owner_lease_token || Ecto.UUID.generate(),
      owner_lease_expires_at: DateTime.add(now, bridge_owner_lease_ttl_seconds(opts), :second),
      last_heartbeat_at: now,
      disconnected_at: nil,
      closed_at: nil,
      close_reason: nil,
      updated_at: now
    })
    |> Repo.update!()
  end

  defp insert_new_session!(auth, opts, session_key, owner, _now, preference) do
    now = db_now()

    attrs = %{
      pool_id: auth.pool.id,
      api_key_id: auth.api_key.id,
      session_key: session_key,
      status: @session_active,
      close_reason: nil,
      owner_instance_id: owner.node_name,
      owner_instance_boot_id: owner.boot_id,
      owner_lease_token: Ecto.UUID.generate(),
      owner_lease_expires_at: DateTime.add(now, bridge_owner_lease_ttl_seconds(opts), :second),
      last_heartbeat_at: now,
      created_at: now,
      updated_at: now
    }

    %CodexSession{}
    |> session_start_changeset(attrs)
    |> Repo.insert(mode: :savepoint)
    |> case do
      {:ok, %CodexSession{} = session} ->
        put_session_preference(session, preference)

      {:error, %Ecto.Changeset{} = changeset} ->
        recover_session_start_conflict!(changeset, auth, opts, session_key, owner, now)
    end
  end

  # Carries the preferred assignment on the new session's struct only, for the
  # request that opened it: the closed session's (row 270-282) or the previous
  # window's live session's (row 270-283). Nothing is persisted: writing it to
  # `pool_upstream_assignment_id` would make routing filter on it, and on a
  # websocket turn it could even escalate to a hard pin. The conflict-recovery
  # path deliberately does not receive it, because a recovered session already
  # carries its own assignment.
  defp put_session_preference(%CodexSession{} = session, {:recreated, assignment_id}),
    do: %{session | recreated_from_assignment_id: assignment_id}

  defp put_session_preference(%CodexSession{} = session, {:previous_window, assignment_id}),
    do: %{session | previous_window_assignment_id: assignment_id}

  defp put_session_preference(%CodexSession{} = session, nil), do: session

  defp session_start_changeset(%CodexSession{} = session, attrs) do
    session
    |> Ecto.Changeset.change(attrs)
    |> Ecto.Changeset.unique_constraint(:session_key,
      name: :codex_sessions_pool_api_key_session_key_uq
    )
  end

  defp recover_session_start_conflict!(changeset, auth, opts, session_key, owner, now) do
    if session_key_unique_constraint?(changeset) do
      case active_session_for_update(auth, session_key, now) do
        %CodexSession{} = session ->
          Logger.info("session_start_conflict_recovered reason=codex_sessions_pool_api_key_session_key_uq outcome=reused_existing_session")

          update_existing_session!(session, auth, opts, owner, now)

        nil ->
          Repo.rollback(@session_start_conflict_error)
      end
    else
      Repo.rollback(changeset)
    end
  end

  defp session_key_unique_constraint?(%Ecto.Changeset{} = changeset) do
    Enum.any?(changeset.constraints, fn constraint ->
      constraint.type == :unique and
        constraint.constraint == "codex_sessions_pool_api_key_session_key_uq"
    end) and
      Keyword.has_key?(changeset.errors, :session_key)
  end

  if Mix.env() == :test do
    defp maybe_test_block_before_session_insert do
      case Process.get({__MODULE__, :before_session_insert_barrier}) do
        {owner_pid, ref} when is_pid(owner_pid) ->
          send(owner_pid, {:session_insert_ready, ref, self()})

          receive do
            {:session_insert_release, ^ref} -> :ok
          end

        _value ->
          :ok
      end
    end
  else
    defp maybe_test_block_before_session_insert, do: :ok
  end

  defp maybe_bind_session_assignment!(%CodexSession{} = session, opts, now) do
    case pool_upstream_assignment_id(opts) do
      assignment_id when is_binary(assignment_id) ->
        bind_session_assignment!(session, assignment_id, now)

      _value ->
        session
    end
  end

  defp bind_session_assignment!(
         %CodexSession{pool_upstream_assignment_id: assignment_id} = session,
         assignment_id,
         _now
       )
       when is_binary(assignment_id),
       do: session

  defp bind_session_assignment!(
         %CodexSession{pool_upstream_assignment_id: nil} = session,
         assignment_id,
         now
       ) do
    session
    |> Ecto.Changeset.change(%{
      pool_upstream_assignment_id: assignment_id,
      last_heartbeat_at: now,
      updated_at: now
    })
    |> Repo.update!()
  end

  defp bind_session_assignment!(%CodexSession{} = session, _assignment_id, _now), do: session

  # A session is scoped to `(pool_id, api_key_id, session_key)`, as the unique
  # index `codex_sessions_pool_api_key_session_key_uq` is. The client sends the
  # same window and session headers whichever key it holds, so a second key of
  # the Pool that sends them opens its own session instead of re-owning the
  # first key's row (plain transports) or being refused `owner_unavailable`
  # while the first key's lease lives (owner forwarding) (findings#255).
  defp active_session_for_update(auth, session_key, now) do
    Repo.one(
      from session in CodexSession,
        where:
          session.pool_id == ^auth.pool.id and session.api_key_id == ^auth.api_key.id and
            fragment("lower(?)", session.session_key) == ^String.downcase(session_key) and
            session.status in ^@session_reconnectable_statuses and
            (is_nil(session.owner_lease_expires_at) or session.owner_lease_expires_at > ^now),
        order_by: [desc: session.updated_at, desc: session.created_at],
        limit: 1,
        lock: "FOR UPDATE"
    )
  end

  defp authenticated_owner_attach_requires_existing?(%RequestOptions{
         openai_compatibility: %{source_endpoint: "/v1/responses"},
         continuity: %{authenticated_owner_attach: true, previous_response_id: nil}
       }),
       do: false

  defp authenticated_owner_attach_requires_existing?(%RequestOptions{
         continuity: %{
           authenticated_owner_attach: true,
           accepted_turn_state: nil,
           previous_response_id: previous_response_id,
           session_header: session_header
         }
       }) do
    not is_nil(blank_to_nil(previous_response_id)) or not is_nil(blank_to_nil(session_header))
  end

  defp authenticated_owner_attach_requires_existing?(_opts), do: false

  defp bridge_owner_lease_ttl_seconds(%RequestOptions{} = request_options) do
    case request_options.continuity.bridge_owner_lease_ttl_seconds do
      seconds when is_integer(seconds) and seconds > 0 -> seconds
      _value -> OperationalSettings.current().bridge_owner_lease_ttl_seconds
    end
  end

  # A turn state the client sent outranks every other identity. One the Pooler
  # issued for a websocket upgrade that carried none only names that
  # connection, so the client's window or session header outranks it and a
  # reconnect on the same window keys the same session, as HTTP does
  # (findings#255); it still keys a connection that sent no identity at all.
  defp session_key(%RequestOptions{} = request_options) do
    request_options
    |> client_turn_state_session_key()
    |> Kernel.||(session_header_session_key(request_options))
    |> Kernel.||(issued_turn_state_session_key(request_options))
    |> Kernel.||(request_options.continuity.session_key |> blank_to_nil())
    |> Kernel.||(Ecto.UUID.generate())
  end

  @spec client_turn_state_session_key(RequestOptions.t()) :: String.t() | nil
  defp client_turn_state_session_key(%RequestOptions{continuity: %{pooler_issued_turn_state?: true}}),
    do: nil

  defp client_turn_state_session_key(%RequestOptions{} = request_options),
    do: turn_state_session_key(request_options)

  @spec issued_turn_state_session_key(RequestOptions.t()) :: String.t() | nil
  defp issued_turn_state_session_key(%RequestOptions{continuity: %{pooler_issued_turn_state?: true}} = request_options),
    do: turn_state_session_key(request_options)

  defp issued_turn_state_session_key(%RequestOptions{}), do: nil

  @spec turn_state_session_key(RequestOptions.t()) :: String.t() | nil
  defp turn_state_session_key(%RequestOptions{continuity: %{accepted_turn_state: turn_state}}) do
    case blank_to_nil(turn_state) do
      nil -> nil
      value -> "x-codex-turn-state:" <> safe_hash(value)
    end
  end

  defp session_header_session_key(%RequestOptions{
         continuity: %{session_header_source: "x-codex-window-id", session_header: session_header}
       }) do
    window_session_key(session_header)
  end

  defp session_header_session_key(%RequestOptions{continuity: %{session_header: session_header}}) do
    blank_to_nil(session_header)
  end

  @doc false
  @spec window_session_key(String.t() | nil) :: String.t() | nil
  def window_session_key(header) do
    case blank_to_nil(header) do
      nil -> nil
      value -> "x-codex-window-id:" <> safe_hash(value)
    end
  end

  defp safe_hash(value) when is_binary(value) do
    :crypto.hash(:sha256, value)
    |> Base.encode16(case: :lower)
  end

  defp pool_upstream_assignment_id(%RequestOptions{} = request_options) do
    request_options.file_bridge.pool_upstream_assignment_id
  end

  defp blank_to_nil(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  defp blank_to_nil(_value), do: nil

  defp now, do: db_now()

  defp unwrap_ok_transaction({:ok, :ok}), do: :ok
  defp unwrap_ok_transaction({:error, reason}), do: {:error, reason}

  defp unwrap_transaction({:ok, value}), do: {:ok, value}
  defp unwrap_transaction({:error, reason}), do: {:error, reason}
end
