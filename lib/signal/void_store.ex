defmodule Signal.VoidStore do

    use GenServer
    alias Signal.Snapshot
    alias Signal.Stream.Event
    alias Signal.VoidStore, as: Store

    @behaviour Signal.Store

    defstruct [cursor: 0, events: [], snapshots: %{}, states: %{}, streams: %{}, indices: %{}, subscriptions: []]

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
    def handle_call({:publish, staged}, _from, %Store{cursor: prev}=store) do
        store = Enum.reduce(staged, store, &(handle_publish(&2, &1)))
        %{events: events} = store
        events = Enum.slice(events, prev, length(events))
        {:reply, {:ok, events}, store}
    end

    @impl true
    def handle_call({:record, %Snapshot{id: id}=snapshot}, _from, %Store{}=store) do
        store = 
            Map.update!(store, :snapshots, fn snapshots ->  
                snaps = 
                    Map.get(snapshots, id, %{})
                    |> Map.put(snapshots.version, snapshot)

                Map.put(snapshots, id, snaps)
            end)
        {:reply, {:ok, id}, store}
    end

    @impl true
    def handle_call({:snapshot, iden, _opts}, _from, %Store{}=store) do
        snapshot = 
            store
            |> Map.get(:snapshots)
            |> Map.get(iden, %{})
            |> Map.values()
            |> Enum.max_by(&(Map.get(&1, :version)), fn -> nil end)

        {:reply, snapshot, store}
    end

    @impl true
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
    def handle_cast({:broadcast, event}, %Store{}=store) do
        subs = Enum.map(store.subscriptions, fn sub -> 
            push_event(sub, event)
        end)
        {:noreply, %Store{store | subscriptions: subs}}
    end

    @impl true
    def handle_cast({:ack, pid, number}, %Store{}=store) do
        store = handle_ack(store, pid, number)
        {:noreply, store}
    end

    @impl true
    def cursor(_app) do
        GenServer.call(__MODULE__, {:state, :cursor}, 5000)
    end

    @impl true
    def publish(staged, _opts \\ [])
    def publish(staged, _opts) when is_list(staged) do
        case GenServer.call(__MODULE__, {:publish, staged}, 5000) do
            {:ok, events} ->
                Enum.map(events, fn event -> 
                    GenServer.cast(__MODULE__, {:broadcast, event})
                end)

                :ok
            error ->
                error
        end
    end

    def publish(staged, opts) do
        List.wrap(staged)
        |> publish(opts)
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

    def subscribe(nil, opts) when is_list(opts) do
        GenServer.call(__MODULE__, {:subscribe, opts}, 5000)
    end

    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        opts = [handle: handle] ++ opts
        GenServer.call(__MODULE__, {:subscribe, opts}, 5000)
    end

    @impl true
    def unsubscribe(_opts \\ []) do
        GenServer.call(__MODULE__, :unsubscribe, 5000)
    end

    @impl true
    def subscription(_opts \\ []) do
        GenServer.call(__MODULE__, :subscription, 5000)
    end

    @impl true
    def acknowledge(number, _opts \\ []) do
        GenServer.cast(__MODULE__, {:ack, self(), number})
    end

    @impl true
    def record(%Snapshot{}=snapshot, _opts) do
        GenServer.call(__MODULE__, {:record, snapshot}, 500)
    end

    @impl true
    def snapshot(iden, opts) do
        GenServer.call(__MODULE__, {:snapshot, iden, opts}, 500)
    end

    @impl true
    def stream_position(stream, _opts \\ []) when is_tuple(stream) do
        %{position: position} =
            GenServer.call(__MODULE__, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream) == stream))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{position: 0} end)
        position
    end

    def list_events(_app, topics, position, count) 
    when is_integer(position) and is_integer(count) do
        GenServer.call(__MODULE__, {:list_events, topics, position, count}, 5000)
    end

    defp push_event(%{handle: handle, syn: syn, ack: ack}=sub, _event)
    when (not is_nil(handle)) and (syn > ack) do
        sub
    end

    defp push_event(%{from: position}=sub, %{number: number})
    when is_integer(position) and position > number do
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

    defp push_next(%Store{events: events}, %{ack: ack}=sub) do
        event = Enum.find(events, &(Map.get(&1, :number) > ack))
        if event do
            push_event(sub, event)
        else
            sub
        end
    end

    defp handle_publish(%Store{}=store, staged) do
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
                    data: event.data,
                    type: event.type,
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
        %Store{store | streams: streams, cursor: cursor, events: events}
    end

end
