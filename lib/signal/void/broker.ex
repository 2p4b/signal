defmodule Signal.Void.Broker do

    use GenServer
    alias Signal.Void.Repo
    alias Signal.Void.Broker
    alias Signal.Stream.Event

    require Logger

    defstruct [
        cursor: 0, 
        handle: nil,
        position: 0,
        subscriptions: []
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
    def handle_call({:state, prop}, _from, %Broker{}=store) do
        {:reply, Map.get(store, prop), store} 
    end

    @impl true
    def handle_call({:subscription, handle}, {pid, _ref}, %Broker{subscriptions: subs}=store) do
        subscription = Enum.find(subs, fn sub -> 
            sub.id == pid and sub.handle == handle
        end)
        {:reply, subscription, store} 
    end

    @impl true
    def handle_call({:subscribe, handle, opts}, {pid, _ref}=from, %Broker{}=store) do
        %Broker{subscriptions: subscriptions} = store

        subscription = Enum.find(subscriptions, fn sub -> 
            sub.id == pid and sub.handle == handle
        end)

        if is_nil(subscription) do
            subscription = create_subscription(store, handle, pid, opts)
            GenServer.reply(from, {:ok, subscription})
            position = subscription.from

            subscriptions =
                Repo.events()
                |> Enum.filter(&(Map.get(&1, :number) > position))
                |> Enum.find_value(subscription, fn event -> 
                    sub = push_event(subscription, event) 
                    if sub.syn == subscription.syn do
                        false
                    else
                        sub
                    end
                end)
                |> List.wrap()
                |> Enum.concat(subscriptions)

            {:noreply, %Broker{store | subscriptions: subscriptions}} 
        else
            {:noreply, subscription, store}
        end
    end

    @impl true
    def handle_call({:unsubscribe, handle}, {pid, _ref}, %Broker{}=store) do
        subscriptions = Enum.filter(store.subscriptions, fn sub -> 
            not(sub.id == pid and sub.handle == handle)
        end)
        {:reply, :ok, %Broker{store | subscriptions: subscriptions}} 
    end

    @impl true
    def handle_info({:next, pid}, %Broker{}=broker) do
        %Broker{subscriptions: subs} =broker
        index = Enum.find_index(subs, &(Map.get(&1, :id) == pid))
        if is_nil(index) do
            {:noreply, broker}
        else
            subs =
                broker
                |> Map.get(:subscriptions)
                |> List.update_at(index, fn sub -> 
                    push_next(broker, sub)
                end)
            {:noreply, %Broker{broker| subscriptions: subs}}
        end
    end

    @impl true
    def handle_cast({:broadcast, %{number: number}=event}, %Broker{}=store) do
        info = """

        [BROKER] published #{inspect(event.type)}
        """
        Logger.info(info)
        subs = Enum.map(store.subscriptions, fn sub -> 
            push_event(sub, event)
        end)
        {:noreply, %Broker{store | subscriptions: subs, cursor: number}}
    end

    @impl true
    def handle_cast({:ack, handle, pid, number}, %Broker{}=broker) do
        broker = handle_ack(broker, handle, pid, number)
        {:noreply, broker}
    end

    defp push_event(%{syn: syn, ack: ack}=sub, _ev)
    when syn != ack do
        sub
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
            Process.send(sub.id, event, []) 
            Map.put(sub, :syn, number)
        else
            sub
        end
    end


    defp create_subscription(%Broker{cursor: cursor}, handle, id, opts) do
        from = Keyword.get(opts, :from, cursor)
        topics = Keyword.get(opts, :topics, [])
        stream = Keyword.get(opts, :stream, nil)
        log = Keyword.get(opts, :log, false)
        %{
            id: id,
            log: log,
            ack: from,
            syn: from,
            from: from,
            handle: handle,
            stream: stream,
            topics: topics,
        }
    end

    defp handle_ack(%Broker{}=store, handle, pid, number) do
        %Broker{subscriptions: subscriptions, cursor: cursor} = store
        index = Enum.find_index(subscriptions, fn sub -> 
            sub.id == pid and sub.handle == handle
        end)
        if is_nil(index) do
            store
        else
            subscriptions = List.update_at(subscriptions, index, fn subscription -> 
                if cursor > subscription.ack do
                    Process.send(self(), {:next, subscription.id}, [])
                end
                Map.put(subscription, :ack, number)
            end)
            %Broker{store| subscriptions: subscriptions}
        end
    end

    defp push_next(%Broker{}, %{ack: ack}=sub) do
        event = 
            Repo.events()
            |> Enum.find(&(Map.get(&1, :number) > ack))

        if event do
            push_event(sub, event)
        else
            sub
        end
    end

    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        GenServer.call(__MODULE__, {:subscribe, handle, opts}, 5000)
    end

    def unsubscribe(handle, _opts) do
        GenServer.call(__MODULE__, {handle, :unsubscribe}, 5000)
    end

    def subscription(handle, _opts \\ []) do
        GenServer.call(__MODULE__, {handle, :subscription}, 5000)
    end

    def acknowledge(handle, number) do
        GenServer.cast(__MODULE__, {:ack, handle, self(), number})
    end

end
