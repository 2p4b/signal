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
    def handle_call(:subscription, {pid, _ref}, %Store{subscriptions: subs}=store) do
        subscription = Enum.find(subs, &(Map.get(&1, :pid) == pid))
        {:reply, subscription, store} 
    end

    @impl true
    def handle_call({:subscribe, opts}, {pid, _ref}=from, %Store{}=store) do
        %Store{subscriptions: subscriptions, events: events} = store
        subscription = Enum.find(subscriptions, &(Map.get(&1, :pid) == pid))
        if is_nil(subscription) do
            subscription = create_subscription(store, pid, opts)
            GenServer.reply(from, {:ok, subscription})
            position = subscription.from
            subscriptions =
                Enum.filter(events, &(Map.get(&1, :number) > position))
                |> Enum.reduce(subscription, fn event, sub -> 
                    push_event(sub, event)
                end)
                |> List.wrap()
                |> Enum.concat(subscriptions)

            {:noreply, %Store{store | subscriptions: subscriptions}} 
        else
            {:noreply, subscription, store}
        end
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

        subs =
            Enum.reduce(events, store.subscriptions, fn event, subs -> 
                Enum.map(subs, &(push_event(&1, event)))
            end)

        events = store.events ++ events

        store = %Store{store | 
            cursor: cursor, 
            events: events, 
            subscriptions: subs
        }

        {:reply, {:ok, cursor}, store}
    end

    def handle_info({:ack, {pid, number}}, %Store{}=store) do
        store = handle_ack(store, pid, number)
        {:noreply, store}
    end

    def handle_info({:next, pid}, %Store{}=store) do
        %Store{subscriptions: subs} = store
        index = Enum.find_index(subs, &(Map.get(&1, :pid) == pid))
        if is_nil(index) do
            {:noreply, store}
        else
            subs =
                store
                |> Map.get(:subscriptions)
                |> List.update_at(index, fn sub -> 
                    push_next(store, sub)
                end)
            {:noreply, %Store{store | subscriptions: subs}}
        end
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
    def subscribe() do
        subscribe([])
    end

    @impl true
    def subscribe(opts) when is_list(opts) do
        subscribe(nil, [])
    end

    @impl true
    def subscribe(handle, opts \\ [])

    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        opts = [handle: handle] ++ opts
        GenServer.call(__MODULE__, {:subscribe, opts}, 5000)
    end

    @impl true
    def unsubscribe() do
        GenServer.call(__MODULE__, :unsubscribe, 5000)
    end

    @impl true
    def subscription() do
        GenServer.call(__MODULE__, :subscription, 5000)
    end

    @impl true
    def stream_position(stream, name \\ __MODULE__) 
    when is_atom(name) and is_tuple(stream) do
        %{position: position} =
            GenServer.call(name, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream) == stream))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{position: 0} end)
        position
    end

    @impl true
    def list_events(_app, topics, position, count) 
    when is_integer(position) and is_integer(count) do
        GenServer.call(__MODULE__, {:list_events, topics, position, count}, 5000)
    end

    defp push_event(%{from: position}=sub, %{number: number})
    when position > number do
        sub
    end

    defp push_event(%{stream: s_stream}=sub, %{stream: e_stream}=event) do
        %{topics: topics} = sub
        %{topic: topic, number: number} = event
        {e_stream_type, _stream_id} = e_stream

        valid_stream =
            cond do
                # All streams
                is_nil(s_stream) ->
                    true

                # Same stream type 
                is_atom(s_stream) ->
                    s_stream == e_stream_type

                # Same stream 
                is_tuple(s_stream) ->
                    e_stream == s_stream

                true ->
                    false
            end

        valid_topic =
            if length(topics) == 0 do
                true
            else
                if topic in topics do
                    true
                else
                    false
                end
            end

        if valid_stream and valid_topic do
            Process.send(sub.pid, event, []) 
            Map.put(sub, :syn, number)
        else
            sub
        end
    end


    defp create_subscription(%Store{cursor: cursor}, pid, opts) do
        from = Keyword.get(opts, :from, cursor)
        topics = Keyword.get(opts, :topics, [])
        stream = Keyword.get(opts, :stream, nil)
        handle = Keyword.get(opts, :handle, nil)
        %{
            pid: pid,
            ack: from,
            syn: from,
            from: from,
            handle: handle,
            stream: stream,
            topics: topics,
        }
    end

    defp handle_ack(%Store{}=store, pid, number) do
        %Store{subscriptions: subscriptions, cursor: cursor} = store
        index = Enum.find_index(subscriptions, &(Map.get(&1, :pid) == pid))
        if is_nil(index) do
            store
        else
            subscriptions = List.update_at(subscriptions, index, fn subscription -> 
                if cursor > subscription.ack do
                    Process.send(self(), {:next, subscription.pid}, [])
                end
                Map.put(subscription, :ack, number)
            end)
            %Store{store| subscriptions: subscriptions}
        end
    end

    defp push_next(%Store{events: events}=store, %{ack: ack}=sub) do
        event = Enum.find(events, &(Map.put(&1, :number) > ack))
        push_event(sub, event)
    end

end
