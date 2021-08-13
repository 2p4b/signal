defmodule Signal.VoidStore do

    use GenServer
    alias Signal.Log
    alias Signal.Events.Event
    alias Signal.Stream.History
    alias Signal.VoidStore, as: Store

    @behaviour Signal.Store

    defstruct [cursor: 0, events: [], states: %{}, indices: %{}, subscriptions: []]

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
    def handle_call({:state, prop}, _from, %Store{}=store) do
        {:reply, Map.get(store, prop), store} 
    end

    @impl true
    def handle_call({:subscribe, opts}, {pid, _ref}=from, %Store{}=store) do
        subscription = create_subscription(pid, opts)
        GenServer.reply(from, {:ok, subscription})
        subscriptions = List.wrap(subscription) ++ store.subscriptions
        Enum.filter(store.events, fn event -> 
            event.number > index
        end)
        |> Enum.each(fn event -> 
            Process.send(pid, event, []) 
        end)
        {:noreply, %Store{store | subscriptions: subscriptions}} 
    end

    @impl true
    def handle_call(:unsubscribe, {pid, _ref}, %Store{}=store) do
        subscriptions = Enum.filter(store.subscriptions, fn %{pid: spid} -> 
            spid != pid 
        end)
        {:reply, :ok, %Store{store | subscriptions: subscriptions}} 
    end

    @impl true
    def handle_call({:get_event, number}, _from, %Store{events: events}=store) do
        {:reply, Enum.find(events, &(Map.get(&1, :number) == number)), store} 
    end

    @impl true
    def handle_call({:get_state, id, version}, _from, %Store{states: states}=store) 
    when is_binary(id) do
        state = 
            case {Map.fetch(states, id), version} do
                {:error, _version} -> nil

                {{:ok, partition}, :min} -> 
                    version =
                        partition
                        |> Map.keys() 
                        |> Enum.min()

                    case Map.fetch(partition, version) do
                        {:ok, val} ->
                            {version, val}

                        :error ->
                            nil
                    end

                {{:ok, partition}, :max} -> 
                    max =
                        partition
                        |> Map.keys() 
                        |> Enum.max()

                    case Map.fetch(partition, max) do
                        {:ok, val} ->
                            {max, val}

                        :error ->
                            nil
                    end


                {{:ok, partition}, version} -> 
                    case Map.fetch(partition, version) do
                        {:ok, val} ->
                            {version, val}

                        :error ->
                            nil
                    end
            end
        {:reply, state, store} 
    end

    @impl true
    def handle_call({:set_state, id, version, state}, _from, %Store{states: states}=store) do
        partition = 
            states
            |> Map.get(id, %{}) 
            |> Map.put(version, state)
        states = Map.put(states, id, partition)
        {:reply, {:ok, version}, %Store{store | states: states}} 
    end

    @impl true
    def handle_call({:get_index, id}, _from, %Store{ indices: indices}=store) 
    when is_binary(id) do
        {:reply, Map.get(indices, id), store} 
    end

    @impl true
    def handle_call({:set_index, id, index}, _from, %Store{indices: indices}=store) 
    when is_binary(id) and is_integer(index) do
        indices = Map.put(indices , id, index)
        {:reply, {:ok, index}, %Store{store | indices: indices}} 
    end

    @impl true
    def handle_call({:list_events, topics, position, count}, _from, %Store{}=store) 
    when is_list(topics) and is_integer(position) and is_integer(count) do
        events = 
            store.events
            |> Enum.filter(fn %Event{topic: topic, number: number} -> 
                Enum.member?(topics, topic) and number > position
            end)
            |> Enum.take(count)

        {:reply, events, store} 
    end

    @impl true
    def handle_call({:next, position, opts}, _from, %Store{}=store) 
    when is_integer(position) do
        {:ok, stream} = Keyword.fetch(opts, :stream)
        event = 
            store.events
            |> Enum.find(fn %Event{stream: estream, number: number} -> 
                stream == estream and number > position
            end)
        {:reply, event, store} 
    end

    @impl true
    def handle_call({:record, %Log{cursor: cursor}=log}, from, %Store{}=store) do

        store =
            log.states
            |> Enum.reduce(store, fn {id, version, value}, store -> 
                handle_call({:set_state, id, version, value}, from, store) 
                |> elem(2)
            end)

        store =
            log.indices
            |> Enum.reduce(store, fn {id, index}, store -> 
                handle_call({:set_index, id, index}, from, store) 
                |> elem(2)
            end)

        events =
            log.streams
            |> Enum.reduce([], fn %History{events: events},  acc -> 
                events ++ acc
            end)
            |> Enum.sort(fn (%Event{number: a}, %Event{number: b}) -> a <= b end)

        Enum.each(events, fn event -> 
            Enum.each(store.subscriptions, fn %{pid: pid} -> 
                Process.send(pid, event, []) 
            end)
        end)

        events = store.events ++ events

        store = %Store{store | cursor: cursor, events: events}

        {:reply, {:ok, cursor}, store}
    end

    @impl true
    def cursor(_app) do
        GenServer.call(__MODULE__, {:state, :cursor}, 5000)
    end

    @impl true
    def record(_app, log) do
        GenServer.call(__MODULE__, {:record, log}, 5000)
    end

    @impl true
    def get_state(_app, id) do
        GenServer.call(__MODULE__, {:get_state, id, 0}, 5000)
    end

    @impl true
    def get_state(_app, id, version) 
    when version in [:max, :min] or is_integer(version) do
        GenServer.call(__MODULE__, {:get_state, id, version}, 5000)
    end

    @impl true
    def set_state(app, id, state) do
        set_state(app, id, 0, state)
    end

    @impl true
    def set_state(_app, id, version, state) when is_integer(version) do
        GenServer.call(__MODULE__, {:set_state, id, version, state}, 5000)
    end

    @impl true
    def get_index(_app, handler) do
        GenServer.call(__MODULE__, {:get_index, handler}, 5000)
    end

    @impl true
    def set_index(_app, handler, position) when is_integer(position) do
        GenServer.call(__MODULE__, {:set_index, handler, position}, 5000)
    end

    @impl true
    def next(_app, position, opts \\ []) do
        GenServer.call(__MODULE__, {:next, position, opts}, 5000)
    end

    @impl true
    def get_event(_app, number) do
        GenServer.call(__MODULE__, {:get_event, number}, 5000)
    end

    @impl true
    def subscribe(_app, opts \\ []) do
        GenServer.call(__MODULE__, {:subscribe, opts}, 5000)
    end

    @impl true
    def unsubscribe(_app) do
        GenServer.call(__MODULE__, :unsubscribe, 5000)
    end

    @impl true
    def list_events(_app, topics, position, count) 
    when is_integer(position) and is_integer(count) do
        GenServer.call(__MODULE__, {:list_events, topics, position, count}, 5000)
    end

    def create_subscription(pid, opts \\ []) do
        from = Keyword.get(opts, :from, 0)
        stream = Keyword.get(opts, :stream, nil)
        topics = Keyword.get(opts, :topics, [])
        %{
            pid: pid,
            from: from,
            stream: stream,
            topics: topics,
        }
    end

end
