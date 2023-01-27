defmodule Signal.Void.Repo do

    use GenServer
    alias Signal.Snapshot
    alias Signal.Void.Repo

    defstruct [
        cursor: 0, 
        events: [], 
        effects: %{}, 
        streams: %{}, 
        handlers: %{},
        snapshots: %{}, 
    ]

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
    def handle_call({:get_event, number}, _from, %Repo{}=store) do
        event = 
            store.events
            |> Enum.find(&(Map.get(&1, :number) == number))
        {:reply, event, store} 
    end

    @impl true
    def handle_call({:get_effect, uuid}, _from, %Repo{}=repo) do
        {:reply, Map.get(repo.effects, uuid), repo} 
    end

    @impl true
    def handle_call({:save_effect, effect}, _from, %Repo{}=repo) do
        effects = Map.put(repo.effects, effect.uuid, effect)
        {:reply, :ok, %Repo{repo| effects: effects}} 
    end

    @impl true
    def handle_call({:delete_effect, uuid}, _from, %Repo{}=repo) do
        effects = Map.delete(repo.effects, uuid)
        {:reply, :ok, %Repo{repo| effects: effects}} 
    end

    @impl true
    def handle_call({:commit, transaction}, from, %Repo{}=store) do
        store = 
            transaction.staged
            |> Enum.reduce(store, &(handle_publish(&2, &1)))

        {:reply, _, store} =
            transaction.snapshots
            |> Enum.reduce({:reply, :ok, store}, fn snaphot, {_, _, store} -> 
               handle_call({:record, snaphot}, from, store)
            end)

        {:reply, :ok, store}
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
    def handle_call({:handler_acknowledge, handler, number, _opts}, _from, %Repo{}=store) do
        %Repo{handlers: handlers} = store
        handlers = Map.put(handlers, handler, number)
        {:reply, {:ok, number}, %Repo{store | handlers: handlers} }
    end

    @impl true
    def handle_call({:handler_position, handler, _opts}, _from, %Repo{}=store) do
        {:reply, Map.get(store.handlers, handler), store} 
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

    def get_cursor() do
        GenServer.call(__MODULE__, {:state, :cursor}, 5000)
    end

    def list_events(opts \\ []) do
        rrange = Keyword.get(opts, :range, [])
        topics = Keyword.get(opts, :topics, [])
        streams = Keyword.get(opts, :streams, [])
        range =
            case Signal.Store.Helper.range(rrange) do
                [lower, upper, :asc] ->
                    Range.new(lower, cast_max(upper))
                [lower, upper, :desc] ->
                    Range.new(cast_max(upper), lower)
            end

        GenServer.call(__MODULE__, {:state, :events}, 5000)
        |> Enum.filter(fn event -> 
                event.number in range and Signal.Store.Helper.event_is_valid?(event, streams, topics)
        end)
    end

    def get_event(number) do
        GenServer.call(__MODULE__, {:get_event, number}, 5000)
    end

    defp cast_max(max) do
        if is_integer(max) do 
            max 
        else 
            get_cursor() 
        end
    end

    def read_events(callback, opts \\ []) do
        rrange = Keyword.get(opts, :range, [])
        topics = Keyword.get(opts, :topics, [])
        streams = Keyword.get(opts, :streams, [])
        range =
            case Signal.Store.Helper.range(rrange) do
                [lower, upper, :asc] ->
                    Range.new(lower, cast_max(upper))
                [lower, upper, :desc] ->
                    Range.new(cast_max(upper), lower)
            end
        range
        |> Enum.find_value(fn number -> 
            event = get_event(number)
            if event do
                if Signal.Store.Helper.event_is_valid?(event, streams, topics) do
                    case callback.(event) do
                        :stop -> true
                        _ -> false
                    end
                end
            else
                true
            end
        end)
        :ok
    end

    def delete_snapshot(snap, opts) do
        GenServer.call(__MODULE__, {:purge, snap, opts}, 5000)
    end

    def record_snapshot(%Snapshot{}=snapshot, _opts) do
        GenServer.call(__MODULE__, {:record, snapshot}, 500)
    end

    def get_snapshot(iden, opts) do
        GenServer.call(__MODULE__, {:snapshot, iden, opts}, 500)
    end

    def handler_position(handler, opts\\[]) do
        GenServer.call(__MODULE__, {:handler_position, handler, opts}, 500)
    end

    def handler_acknowledge(handler, number, opts\\[]) do
        GenServer.call(__MODULE__, {:handler_acknowledge, handler, number, opts}, 500)
    end

    def stream_position(stream, _opts \\ []) when is_tuple(stream) do
        %{index: position} =
            GenServer.call(__MODULE__, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream) == stream))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{index: 0} end)
        position
    end

    defp handle_publish(%Repo{}=store, staged) do
        %{streams: streams, cursor: cursor} = store
        %{stream: stream, events: events, position: version} = staged

        {stream_id, _} = stream
        store_stream = Map.get(streams, stream_id, %{
            id: stream_id,
            position: 0,
        })

        events = store.events ++ events

        streams = Map.put(streams, stream_id, Map.put(store_stream, :position, version))
        %Repo{store | streams: streams, cursor: cursor, events: events}
    end

    @impl true
    def terminate(_reason, _state) do
        :ok
    end

end
