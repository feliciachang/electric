defmodule Electric.Replication.Postgres.LogicalReplicationProducerTest do
  use ExUnit.Case, async: false
  import Mock

  alias Electric.Postgres.Extension.SchemaLoader
  alias Electric.Postgres.LogicalReplication
  alias Electric.Postgres.LogicalReplication.Messages
  alias Electric.Postgres.Lsn

  alias Electric.Replication.Changes.{NewRecord, UpdatedRecord, Transaction}
  alias Electric.Replication.Connectors
  alias Electric.Replication.Postgres.Client
  alias Electric.Replication.Postgres.LogicalReplicationProducer

  @uuid_oid 2950
  @varchar_oid 1043

  setup_with_mocks([
    {Client, [:passthrough],
     [
       with_conn: fn _, fun -> fun.(:conn) end,
       connect: fn _ -> {:ok, :conn} end,
       start_replication: fn :conn, _, _, _, _ -> :ok end,
       create_main_slot: fn :conn, name -> {:ok, name} end,
       create_temporary_slot: fn :conn, _main_name, tmp_name -> {:ok, tmp_name, %Lsn{}} end,
       current_lsn: fn :conn -> {:ok, %Lsn{}} end,
       advance_replication_slot: fn _, _, _ -> :ok end,
       set_display_settings_for_replication: fn _ -> :ok end,
       get_server_versions: fn :conn -> {:ok, {"", "", ""}} end
     ]},
    {Connectors, [:passthrough],
     [
       get_replication_opts: fn _ -> %{publication: "mock_pub", slot: "mock_slot"} end,
       get_connection_opts: fn _ -> %{ip_addr: {0, 0, 0, 1}} end,
       get_connection_opts: fn _, _ -> %{ip_addr: {0, 0, 0, 2}} end
     ]},
    {SchemaLoader, [:passthrough],
     [
       count_electrified_tables: fn _ -> {:ok, 0} end
     ]}
  ]) do
    {:ok, %{}}
  end

  test "Producer complies a transaction into a single message" do
    {_, events} =
      begin()
      |> relation("entities", id: @uuid_oid, data: @varchar_oid)
      |> insert("entities", ["test", "value"])
      |> commit_and_get_messages()
      |> process_messages(
        initialize_producer(),
        &LogicalReplicationProducer.handle_info/2
      )

    assert [%Transaction{} = transaction] = events

    assert [%NewRecord{record: %{"id" => "test", "data" => "value"}}] = transaction.changes
  end

  test "Producer keeps proper ordering of updates within the transaction for inserts" do
    {_, events} =
      begin()
      |> relation("entities", id: @uuid_oid, data: @varchar_oid)
      |> insert("entities", ["test1", "value"])
      |> insert("entities", ["test2", "value"])
      |> insert("entities", ["test3", "value"])
      |> insert("entities", ["test4", "value"])
      |> commit_and_get_messages()
      |> process_messages(
        initialize_producer(),
        &LogicalReplicationProducer.handle_info/2
      )

    assert [%Transaction{} = transaction] = events
    assert length(transaction.changes) == 4

    assert [
             %NewRecord{record: %{"id" => "test1"}},
             %NewRecord{record: %{"id" => "test2"}},
             %NewRecord{record: %{"id" => "test3"}},
             %NewRecord{record: %{"id" => "test4"}}
           ] = transaction.changes
  end

  test "Producer keeps proper ordering of updates within the transaction for updates" do
    {_, events} =
      begin()
      |> relation("entities", id: @uuid_oid, data: @varchar_oid)
      |> insert("entities", ["test", "1"])
      |> update("entities", ["test", "1"], ["test", "2"])
      |> update("entities", ["test", "2"], ["test", "3"])
      |> update("entities", ["test", "3"], ["test", "4"])
      |> update("entities", ["test", "4"], ["test", "5"])
      |> commit_and_get_messages()
      |> process_messages(
        initialize_producer(),
        &LogicalReplicationProducer.handle_info/2
      )

    assert [%Transaction{} = transaction] = events
    assert length(transaction.changes) == 5

    assert [
             %NewRecord{record: %{"data" => "1"}},
             %UpdatedRecord{record: %{"data" => "2"}, old_record: %{"data" => "1"}},
             %UpdatedRecord{record: %{"data" => "3"}, old_record: %{"data" => "2"}},
             %UpdatedRecord{record: %{"data" => "4"}, old_record: %{"data" => "3"}},
             %UpdatedRecord{record: %{"data" => "5"}, old_record: %{"data" => "4"}}
           ] = transaction.changes
  end

  test "Producer schedules the magic write timer" do
    %LogicalReplicationProducer.State{magic_write_timer: tref} = initialize_producer()
    assert_receive {:timeout, ^tref, :magic_write}, 2_000
  end

  def initialize_producer(demand \\ 100) do
    {:producer, state} =
      LogicalReplicationProducer.init(
        origin: "mock_postgres",
        wal_window: [in_memory_size: 1, resumable_size: 1],
        connection: %{}
      )

    {_, _, state} = LogicalReplicationProducer.handle_demand(demand, state)
    state
  end

  defp process_messages(messages, initial_state, gen_stage_callback)
       when is_function(gen_stage_callback, 2) do
    Enum.reduce(messages, {initial_state, []}, fn msg, {state, events} ->
      {:noreply, new_events, new_state} = gen_stage_callback.(msg, state)
      {new_state, events ++ new_events}
    end)
  end

  defp begin() do
    %{
      lsn: %Lsn{segment: Enum.random(0..0xFF), offset: Enum.random(1..0xFFFFFFFF)},
      actions: [],
      relations: %{}
    }
  end

  defp relation(state, name, columns) do
    relation_id = Enum.random(0..0xFFFFFFFF)

    state
    |> Map.update!(:relations, &Map.put(&1, name, relation_id))
    |> add_action(%Messages.Relation{
      id: relation_id,
      name: name,
      replica_identity: :all_columns,
      namespace: "public",
      columns:
        Enum.map(columns, fn {name, type_oid} ->
          %Messages.Relation.Column{
            flags: [],
            name: Atom.to_string(name),
            type_oid: type_oid,
            type_modifier: 0
          }
        end)
    })
  end

  defp insert(state, relation, data) do
    add_action(state, %Messages.Insert{
      relation_id: Map.fetch!(state.relations, relation),
      tuple_data: data
    })
  end

  defp update(state, relation, old_record, data) when is_list(old_record) do
    add_action(state, %Messages.Update{
      relation_id: Map.fetch!(state.relations, relation),
      old_tuple_data: old_record,
      tuple_data: data
    })
  end

  defp commit_and_get_messages(%{lsn: lsn, actions: actions}) do
    timestamp = DateTime.utc_now()

    begin = %Messages.Begin{
      xid: lsn.segment,
      final_lsn: lsn,
      commit_timestamp: timestamp
    }

    commit = %Messages.Commit{
      commit_timestamp: timestamp,
      lsn: lsn,
      end_lsn: Map.update!(lsn, :offset, &(&1 + 30)),
      flags: []
    }

    ([begin] ++ actions ++ [commit])
    |> Enum.map(&LogicalReplication.encode_message/1)
    |> Enum.map(&{:epgsql, self(), {:x_log_data, 0, 0, &1}})
  end

  defp add_action(state, action), do: Map.update!(state, :actions, &(&1 ++ [action]))
end
