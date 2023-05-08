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
        {:reply, Enum.at(store.events, number - 1), store} 
    end

    @impl true
    def handle_call({:get_stream_event, sid, version}, _from, %Repo{}=store) do
        event = 
            store.events
            |> Enum.find(fn event -> 
                event.stream_id === sid and event.version === version
            end)
        {:reply, event, store} 
    end

    @impl true
    def handle_call({:get_effect, uuid}, _from, %Repo{}=repo) do
        {:reply, Map.get(repo.effects, uuid), repo} 
    end

    @impl true
    def handle_call({:list_effects, namespace}, _from, %Repo{}=store) do
        effects = 
            store.effects
            |> Map.filter(&(Map.get(elem(&1, 1), :namespace) == namespace))
            |> Map.values()
        {:reply, effects, store} 
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
    def handle_call({:commit, transaction}, _from, %Repo{}=repo) do
        repo = 
            transaction.staged
            |> Enum.reduce(repo, &(handle_publish(&2, &1)))
            |> Map.put(:cursor, transaction.cursor)
        {:reply, :ok, repo}
    end

    @impl true
    def handle_call({:record, %Snapshot{}=snapshot}, _from, %Repo{}=repo) do
        %Repo{snapshots: snapshots} = repo
        %Snapshot{id: id} = snapshot

        snapshots = Map.put(snapshots, id, snapshot)
        {:reply, {:ok, id}, %Repo{repo | snapshots: snapshots} }
    end

    @impl true
    def handle_call({:handler_acknowledge, handler, number, _opts}, _from, %Repo{}=repo) do
        %Repo{handlers: handlers} = repo
        handlers = Map.put(handlers, handler, number)
        {:reply, {:ok, number}, %Repo{repo | handlers: handlers} }
    end

    @impl true
    def handle_call({:handler_position, handler, _opts}, _from, %Repo{}=repo) do
        {:reply, Map.get(repo.handlers, handler), repo} 
    end

    @impl true
    def handle_call({:snapshot, id, _opts}, _from, %Repo{}=repo) do
        {:reply, Map.get(repo.snapshots, id), repo}
    end

    @impl true
    def handle_call({:delete_snapshot, id, _opts}, _from, %Repo{}=repo) do
        snapshots = Map.delete(repo.snapshots, id)
        {:reply, :ok, %Repo{repo| snapshots: snapshots}}
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

    def list_stream_events(sid, opts \\ []) do
        rrange = Keyword.get(opts, :range, [])
        topics = Keyword.get(opts, :topics, [])
        range =
            case Signal.Store.Helper.range(rrange) do
                [lower, upper, :asc] ->
                    Range.new(lower, cast_max(upper))
                [lower, upper, :desc] ->
                    Range.new(cast_max(upper), lower)
            end

        GenServer.call(__MODULE__, {:state, :events}, 5000)
        |> Enum.filter(fn event -> 
                event.position in range and Signal.Store.Helper.event_is_valid?(event, [sid], topics)
        end)
    end

    def get_event(number) do
        GenServer.call(__MODULE__, {:get_event, number}, 5000)
    end

    def get_stream_event(sid, version) do
        GenServer.call(__MODULE__, {:get_stream_event, sid, version}, 5000)
    end

    defp cast_max(max) do
        if is_integer(max) do 
            max 
        else 
            case get_cursor() do
                0 -> 1
                value -> value
            end
        end
    end

    def read_stream_events(sid, callback, opts \\ []) do
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
        |> Enum.find_value(fn position -> 
            event = get_stream_event(sid, position)
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

    def record_snapshot(%Snapshot{}=snapshot, _opts) do
        GenServer.call(__MODULE__, {:record, snapshot}, 5000)
    end

    def get_snapshot(id, opts) do
        GenServer.call(__MODULE__, {:snapshot, id, opts}, 5000)
    end

    def delete_snapshot(id, opts) do
        GenServer.call(__MODULE__, {:delete_snapshot, id, opts}, 5000)
    end

    def handler_position(handler, opts\\[]) do
        GenServer.call(__MODULE__, {:handler_position, handler, opts}, 5000)
    end

    def handler_acknowledge(handler, number, opts\\[]) do
        GenServer.call(__MODULE__, {:handler_acknowledge, handler, number, opts}, 5000)
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
        %{stream: stream, events: events, version: version} = staged

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
