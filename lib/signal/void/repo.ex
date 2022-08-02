defmodule Signal.Void.Repo do

    use GenServer
    alias Signal.Snapshot
    alias Signal.Void.Repo
    alias Signal.Stream.Event

    defstruct [cursor: 0, events: [], snapshots: %{}, streams: %{}]

    @doc """
    Starts in memory store.
    """
    def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
        {:ok, struct(__MODULE__, [])}
    end

    @impl true
    def handle_call({:state, prop}, _from, %Repo{}=store) do
        {:reply, Map.get(store, prop), store} 
    end

    @impl true
    def handle_call({:publish, transactions}, _from, %Repo{cursor: prev}=store) do
        store = 
            transactions
            |> Enum.reduce([], fn tran, acc -> acc ++ tran.staged  end)
            |> Enum.reduce(store, &(handle_publish(&2, &1)))
        %{events: events} = store
        events = Enum.slice(events, prev, length(events))
        {:reply, {:ok, events}, store}
    end

    @impl true
    def handle_call({:record, %Snapshot{}=snapshot}, _from, %Repo{}=store) do
        %Repo{snapshots: snapshots} = store
        %Snapshot{id: id, type: type, version: version} = snapshot

        versions =
            snapshots
            |> Map.get({type, id}, %{})
            |> Map.put(version, snapshot)

        snapshots = Map.put(snapshots, id, versions)

        {:reply, {:ok, id}, %Repo{store | snapshots: snapshots} }
    end

    @impl true
    def handle_call({:purge, iden, _opts}, _from, %Repo{}=store) do
        %Repo{snapshots: snapshots} = store
        snapshots = Map.delete(snapshots, iden)
        {:reply, :ok, %Repo{store | snapshots: snapshots} }
    end

    @impl true
    def handle_call({:snapshot, iden, _opts}, _from, %Repo{}=store) do
        snapshot = 
            store
            |> Map.get(:snapshots)
            |> Map.get(iden, %{})
            |> Map.values()
            |> Enum.max_by(&(Map.get(&1, :version)), fn -> nil end)

        {:reply, snapshot, store}
    end

    def cursor() do
        GenServer.call(__MODULE__, {:state, :cursor}, 5000)
    end

    def events() do
        GenServer.call(__MODULE__, {:state, :events}, 5000)
    end

    def event(number) do
        events()
        |> Enum.find(&(Map.get(&1, :number) == number))
    end

    def publish(staged) when is_list(staged) do
        GenServer.call(__MODULE__, {:publish, staged}, 5000)
    end

    def purge(snap, opts) when is_tuple(snap) and is_list(opts) do
        GenServer.call(__MODULE__, {:purge, snap, opts}, 5000)
    end

    def record(%Snapshot{}=snapshot, _opts) do
        GenServer.call(__MODULE__, {:record, snapshot}, 500)
    end

    def snapshot(iden, opts) do
        GenServer.call(__MODULE__, {:snapshot, iden, opts}, 500)
    end

    def stream_position(stream, _opts \\ []) when is_tuple(stream) do
        %{position: position} =
            GenServer.call(__MODULE__, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream) == stream))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{position: 0} end)
        position
    end

    defp handle_publish(%Repo{}=store, staged) do
        %{streams: streams, cursor: cursor} = store
        %{stream: stream, events: events, version: version} = staged

        store_stream = Map.get(streams, stream, %{
            id: UUID.uuid4(),
            position: 0,
        })

        %{position: position} = store_stream

        version = if is_nil(version), do: position, else: version

        initial = {[], position, cursor}

        preped = 
            Enum.reduce(events, initial, fn event, {events, position, number} -> 
                number = number + 1
                position = position + 1
                event = %Event{
                    uuid: UUID.uuid4(),
                    stream: stream,
                    number: number,
                    position: position,
                    payload: event.payload,
                    topic: event.topic, 
                    timestamp: event.timestamp,
                    causation_id: event.causation_id,
                    correlation_id: event.correlation_id,
                }
                {events ++ List.wrap(event), position, number}
            end)

        {events, ^version, cursor} = preped 

        events = store.events ++ events

        streams = Map.put(streams, stream, Map.put(store_stream, :position, version))
        %Repo{store | streams: streams, cursor: cursor, events: events}
    end

    @impl true
    def terminate(_reason, _state) do
        :ok
    end

end
