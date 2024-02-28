defmodule Electric.Postgres.CachedWal.Api do
  @moduledoc """
  Behavior for accessing cached wal
  """
  alias Electric.Telemetry.Metrics
  @type lsn :: Electric.Postgres.Lsn.t()

  @typedoc "Position in the cached write-ahead log"
  @type wal_pos :: term()

  @typedoc "Notification reference no notify when new wal segment is available"
  @type await_ref :: reference()

  @typedoc "Wal segment, where segment is just an abstraction term within Electric"
  @type segment :: Electric.Replication.Changes.Transaction.t()

  @type stats :: %{
          transaction_count: non_neg_integer(),
          oldest_transaction_timestamp: DateTime.t() | nil,
          max_cache_size: pos_integer(),
          cache_memory_total: non_neg_integer()
        }

  @callback lsn_in_cached_window?(wal_pos) :: boolean
  @callback get_current_position() :: wal_pos | nil
  @callback next_segment(wal_pos()) ::
              {:ok, segment(), new_position :: wal_pos()} | :latest | {:error, term()}
  @callback request_notification(wal_pos()) :: {:ok, await_ref()} | {:error, term()}
  @callback cancel_notification_request(await_ref()) :: :ok

  @callback serialize_wal_position(wal_pos()) :: binary()
  @callback parse_wal_position(binary()) :: {:ok, wal_pos()} | :error
  @callback compare_positions(wal_pos(), wal_pos()) :: :lt | :eq | :gt

  @callback telemetry_stats() :: stats() | nil

  def default_module,
    do: Application.fetch_env!(:electric, __MODULE__) |> Keyword.fetch!(:adapter)

  @doc """
  Check if the given LSN falls into the caching window maintained by the cached WAL implementation.

  This checks needs to be done for every client. If their LSN is outside of the caching window, we won't be able to
  guarantee data consistency via the replication stream alone.
  """
  @spec lsn_in_cached_window?(module(), wal_pos) :: boolean
  def lsn_in_cached_window?(module \\ default_module(), lsn) do
    module.lsn_in_cached_window?(lsn)
  end

  @doc """
  Get the latest LSN that the cached WAL has seen.

  Returns nil if the cached WAL hasn't processed any non-empty transactions yet.
  """
  @spec get_current_position(module()) :: wal_pos | nil
  def get_current_position(module \\ default_module()) do
    module.get_current_position()
  end

  @doc """
  Get the next segment from the cached WAL from the current position.

  If there's a next segment available, returns it along with the new position for the next read,
  otherwise returns an atom `:latest`. There could be a case where lsn is already too old
  (i.e. out of the cached window), in which case an error will be returned, and the client is expected
  to query source database directly to catch up.
  """
  @spec next_segment(module(), wal_pos()) ::
          {:ok, segment(), new_position :: wal_pos()} | :latest | {:error, :lsn_too_old}
  def next_segment(module \\ default_module(), wal_pos) do
    module.next_segment(wal_pos)
  end

  def compare_positions(module \\ default_module(), wal_pos_1, wal_pos_2),
    do: module.compare_positions(wal_pos_1, wal_pos_2)

  @spec get_transactions(module(), from: wal_pos(), to: wal_pos()) ::
          {:ok, [{segment(), wal_pos()}]} | {:error, :lsn_too_old}
  def get_transactions(module \\ default_module(), from: from_pos, to: to_pos) do
    if lsn_in_cached_window?(module, from_pos) do
      {:ok, Enum.to_list(stream_transactions(module, from: from_pos, to: to_pos))}
    else
      {:error, :lsn_too_old}
    end
  end

  @spec stream_transactions([{:from, any()} | {:to, any()}, ...]) ::
          Enumerable.t({segment(), wal_pos()})
  def stream_transactions(module \\ default_module(), from: from_pos, to: to_pos) do
    Stream.unfold(from_pos, fn from_pos ->
      case next_segment(module, from_pos) do
        {:ok, segment, new_pos} ->
          if module.compare_positions(new_pos, to_pos) != :gt, do: {segment, new_pos}, else: nil

        :latest ->
          nil
      end
    end)
  end

  @doc """
  Request notification to be sent as soon as any segment with position higher than specified shows up.

  The calling process will receive a message in the form of
  `{:cached_wal_notification, ref(), :new_segments_available}`
  as soon as a new segment becomes available in the cache.
  """
  @spec request_notification(module(), wal_pos()) :: {:ok, await_ref()} | {:error, term()}
  def request_notification(module \\ default_module(), wal_pos) do
    module.request_notification(wal_pos)
  end

  @doc """
  Cancel a notification request issued previously by `request_notification/2`.
  """
  @spec cancel_notification_request(module(), await_ref()) :: :ok
  def cancel_notification_request(module \\ default_module(), await_ref) do
    module.cancel_notification_request(await_ref)
  end

  @spec parse_wal_position(module(), binary()) :: {:ok, wal_pos()} | :error
  def parse_wal_position(module \\ default_module(), bin) do
    module.parse_wal_position(bin)
  end

  @spec serialize_wal_position(module(), wal_pos()) :: binary()
  def serialize_wal_position(module \\ default_module(), wal_pos) do
    module.serialize_wal_position(wal_pos)
  end

  @spec emit_telemetry_stats(module()) :: :ok
  def emit_telemetry_stats(module \\ default_module(), event) do
    case module.telemetry_stats() do
      nil -> :ok
      stats -> Metrics.non_span_event(event, stats)
    end
  end
end
