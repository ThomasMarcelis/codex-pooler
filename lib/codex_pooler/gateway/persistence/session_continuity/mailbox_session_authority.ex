defmodule CodexPooler.Gateway.Persistence.SessionContinuity.MailboxSessionAuthority do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Gateway.Payloads.NativeCodexTurnMetadata
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.MailboxAdmissionLocks
  alias CodexPooler.Gateway.Persistence.StatusVocabulary.Session
  alias CodexPooler.Repo

  # The resolver's chain depth: a chain crosses at most one window per link.
  @max_window_history 16

  @type scope :: %{required(:pool_id) => Ecto.UUID.t(), required(:api_key_id) => Ecto.UUID.t(), optional(:native_window_id) => String.t() | nil}
  @type verdict :: :same_session | :expired_replacement | :window_successor | :rejected

  @spec verdict(Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, scope()) :: verdict()
  def verdict(id, id, _scope) when is_binary(id), do: :same_session

  def verdict(previous_id, current_id, scope) when is_binary(previous_id) and is_binary(current_id) do
    rows = Repo.all(from s in CodexSession, where: s.id in ^[previous_id, current_id] and s.pool_id == ^scope.pool_id and s.api_key_id == ^scope.api_key_id)
    previous = Enum.find(rows, &(&1.id == previous_id))
    current = Enum.find(rows, &(&1.id == current_id))

    if previous && current && MailboxAdmissionLocks.coordinated?() do
      MailboxAdmissionLocks.require_sessions!([previous_id, current_id])

      different_session_verdict(previous, current, scope)
    else
      :rejected
    end
  end

  def verdict(_previous_id, _current_id, _scope), do: :rejected

  defp different_session_verdict(previous, current, scope) do
    cond do
      expired_replacement?(previous, current) -> :expired_replacement
      window_successor?(previous, current, Map.get(scope, :native_window_id)) -> :window_successor
      true -> :rejected
    end
  end

  # The caller walks every durable successor and verifies its witness. Prove
  # this stored edge, not an ancestor-to-current shortcut; the fresh tail is
  # checked by verdict/3. Later unrelated work cannot revoke a settled edge.
  @spec edge_verdict(Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, scope()) :: verdict()
  def edge_verdict(previous_id, successor_id, current_id, scope) do
    with true <- MailboxAdmissionLocks.coordinated?(),
         {:ok, rows} <- scoped_sessions([previous_id, successor_id, current_id], scope) do
      MailboxAdmissionLocks.require_sessions!([previous_id, successor_id, current_id])
      historical_edge_verdict(rows[previous_id], rows[successor_id], rows[current_id], scope)
    else
      _unproved -> :rejected
    end
  end

  defp scoped_sessions(ids, scope) do
    rows = Repo.all(from s in CodexSession, where: s.id in ^ids and s.pool_id == ^scope.pool_id and s.api_key_id == ^scope.api_key_id)
    if length(rows) == length(Enum.uniq(ids)), do: {:ok, Map.new(rows, &{&1.id, &1})}, else: :error
  end

  defp historical_edge_verdict(%CodexSession{id: id}, %CodexSession{id: id}, _current, _scope), do: :same_session

  defp historical_edge_verdict(previous, successor, current, scope) do
    cond do
      not historical_session?(previous) or not historical_session?(successor) -> :rejected
      certified_close?(previous) and DateTime.compare(successor.created_at, previous.closed_at) != :lt and same_canonical_key?(previous.id, successor.id) -> :expired_replacement
      historical_window_edge?(previous, successor, current, scope) -> :window_successor
      true -> :rejected
    end
  end

  defp historical_session?(session), do: session.status in Session.reconnectable_statuses() or certified_close?(session)

  defp historical_window_edge?(previous, successor, current, scope) do
    window = Map.get(scope, :native_window_id)

    is_binary(window) and matches_window?(current, window) and
      adjacent_keys?(previous, successor, window, @max_window_history)
  end

  defp adjacent_keys?(previous, successor, window, remaining) when is_integer(remaining) and remaining > 0 do
    case NativeCodexTurnMetadata.previous_window(window) do
      {:ok, earlier} ->
        (matches_window?(previous, earlier) and matches_window?(successor, window)) or
          adjacent_keys?(previous, successor, earlier, remaining - 1)

      :none ->
        false
    end
  end

  defp adjacent_keys?(_previous, _successor, _window, _remaining), do: false

  defp certified_close?(%CodexSession{status: "closed", close_reason: "owner_lease_expired", closed_at: %DateTime{} = closed, owner_lease_expires_at: %DateTime{} = expires}), do: DateTime.compare(expires, closed) != :gt
  defp certified_close?(_session), do: false

  defp expired_replacement?(%CodexSession{status: "closed", close_reason: "owner_lease_expired", closed_at: %DateTime{} = closed_at, owner_lease_expires_at: %DateTime{} = expired_at} = previous, %CodexSession{created_at: %DateTime{} = created_at} = current) do
    current.status in Session.reconnectable_statuses() and
      DateTime.compare(expired_at, closed_at) != :gt and
      DateTime.compare(created_at, closed_at) != :lt and
      same_canonical_key?(previous.id, current.id) and
      not Repo.exists?(from turn in CodexTurn, where: turn.codex_session_id == ^current.id and turn.status == "in_progress")
  end

  defp expired_replacement?(_previous, _current), do: false

  # Claims follow the thread across compaction; sessions follow its window.
  # Both sessions are locked before the live-turn check.
  defp window_successor?(previous, current, window) do
    with {:ok, previous_window} <- NativeCodexTurnMetadata.previous_window(window),
         true <- previous.status in Session.reconnectable_statuses(),
         true <- current.status in Session.reconnectable_statuses(),
         true <- matches_window?(current, window),
         true <- matches_window?(previous, previous_window) do
      not Repo.exists?(from turn in CodexTurn, where: turn.codex_session_id in ^[previous.id, current.id] and turn.status == "in_progress")
    else
      _unproved -> false
    end
  end

  defp matches_window?(session, window), do: String.downcase(session.session_key) == SessionContinuity.window_session_key(window)

  defp same_canonical_key?(previous_id, current_id),
    do: Repo.exists?(from previous in CodexSession, join: current in CodexSession, on: current.id == ^current_id, where: previous.id == ^previous_id and fragment("lower(?) = lower(?)", previous.session_key, current.session_key))
end
