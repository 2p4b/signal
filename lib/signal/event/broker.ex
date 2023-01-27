defmodule Signal.Event.Broker do

    use GenServer
    alias Signal.Event
    alias Signal.Event.Broker
    alias Signal.Event.Supervisor

    require Logger

    defstruct [
        handle: nil,
        worker: nil,
        app: nil,
        ready: true,
        cursor: 0, 
        buffer: [],
        position: 0,
        subscriptions: [],
        topics: [],
        streams: [],
    ]

    @doc """
    Starts in memory store.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name, __MODULE__)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        app = Keyword.get(opts, :app)
        handle = Keyword.get(opts, :handle)
        position = 
            case Signal.Store.Adapter.handler_position(app, handle) do
                nil ->
                    0
                position -> 
                    position
            end
        opts = Keyword.merge([position: position], opts)
        {:ok, struct(__MODULE__, opts)}
    end

    @impl true
    def handle_call({:state, prop}, _from, %Broker{}=broker) do
        {:reply, Map.get(broker, prop), broker} 
    end

    @impl true
    def handle_call(:subscription, {pid, _ref}, %Broker{subscriptions: subs}= broker) do
        subscription = Enum.find(subs, &(Map.get(&1, :id) == pid))
        {:reply, subscription, broker} 
    end

    @impl true
    def handle_call({:subscribe, opts}, {pid, _ref}, %Broker{}=broker) do
        %Broker{subscriptions: subscriptions} = broker
        subscription = Enum.find(subscriptions, &(Map.get(&1, :id) == pid))

        if is_nil(subscription) do
            subscription = create_subscription(broker, pid, opts)
            subscriptions = subscriptions ++ List.wrap(subscription)

            {streams, topics} = collect_streams_and_topics(subscriptions)

            position = 
                case subscriptions do
                    [_sub] ->
                        subscription.ack
                    _ ->
                        broker.position
                end

            broker = %Broker{broker | 
                topics: topics, 
                streams: streams,
                position: position,
                subscriptions: subscriptions, 
            }

            broker = start_worker_stream(broker)

            {:reply, {:ok, subscription}, broker} 
        else
            {:reply, {:ok, subscription}, broker}
        end
    end

    @impl true
    def handle_call(:unsubscribe, {pid, _ref}, %Broker{}=broker) do
        subscriptions = Enum.filter(broker.subscriptions, fn %{pid: spid} -> 
            spid != pid 
        end)
        {:reply, :ok, %Broker{broker| subscriptions: subscriptions}} 
    end

    @impl true
    def handle_call({:ack, pid, number}, _from, %Broker{}=broker) do

        %Broker{handle: handle, position: position} = broker

        broker = handle_ack(broker, pid, number)

        subscription = 
            broker
            |> Map.get(:subscriptions)
            |> Enum.max_by(&(Map.get(&1, :ack)), fn -> 
                %{ack: position, track: false, id: pid} 
            end)

        %{ack: ack, track: track, id: id} = subscription

        if track and (ack > position) and (id == pid) do
            {:ok, ^number} = 
                Signal.Store.Adapter.handler_acknowledge(broker.app, handle, number)

            {:reply, number, broker}
        else
            {:reply, number,  broker}
        end
    end

    @impl true
    def handle_info({:push, %{number: number}=event}, %Broker{}=broker) do

        %Broker{subscriptions: subscriptions} = broker
        index = Enum.find_index(subscriptions, fn sub -> 
            handle?(sub, event)
        end)

        if is_nil(index) do
            broker = 
                broker
                |> struct(%{ready: true}) 
                |> sched_next()

            {:noreply, broker}
        else
            subs = List.update_at(subscriptions, index, fn sub -> 
                send(sub.id, event)
                info = """
                [BROKER] #{broker.handle}
                number: #{event.number}
                published: #{event.topic}
                stream position: #{event.position}
                """
                Logger.info(info)
                Map.put(sub, :syn, number)
            end)
            {:noreply, %Broker{broker | subscriptions: subs, ready: false}}
        end
    end

    @impl true
    def handle_info(%Event{}=event, %Broker{buffer: buffer, worker: nil}=broker) do
        broker = 
            %Broker{broker | 
                buffer: buffer ++ List.wrap(event)
            }
            |> sched_next()

        unless Enum.empty?(broker.buffer) do
            info = """
            [BROKER] #{broker.handle}
            queued: #{event.topic}
            number: #{event.number}
            position: #{event.position}
            """
            Logger.info(info)
        end

        {:noreply, broker}
    end

    @impl true
    def handle_info(%Event{}, %Broker{}=broker) do
        # recived event while worker still pulling events
        # ignore event because event will be pulled
        # eventually
        {:noreply, broker}
    end

    @impl true
    def handle_info({_worker_ref, {:worker, {:done, _pid}}}, %Broker{}=broker) do
        # Attach the broker to recieve 
        # events from the store writer
        broker.app
        |> Signal.Store.Writer.attach()

        # Testament.listern_event()
        # Pull events thats may have fallen through
        config = [
            range: [broker.position + 1],
            topics: broker.topics, 
            streams: broker.streams,
        ] 

        fallen = 
            broker.app
            |> Signal.Store.Adapter.list_events(config)
        broker = %Broker{broker | buffer: fallen, worker: nil}
        {:noreply, broker |> sched_next() }
    end

    @impl true
    def handle_info({_worker_ref, _result}, %Broker{}=broker) do
        {:noreply, broker}
    end

    @impl true
    def handle_info({:DOWN, _ref, :process, _pid, _status}, %Broker{}=broker) do
        # Handle the worker shuting down
        {:noreply, broker}
    end

    defp handle?(%{syn: syn, ack: ack}, _ev)
    when syn != ack do
        false
    end

    defp handle?(%{handle: handle, syn: syn, ack: ack}, _event)
    when (not is_nil(handle)) and (syn > ack) do
        false
    end

    defp handle?(%{ack: position}, %{number: number})
    when is_integer(position) and position > number do
        false
    end

    defp handle?(%{stream: sstream}, %{stream: estream}) 
    when not(is_nil(sstream)) and sstream != estream do
        false
    end

    defp handle?(%{topics: topics}, %{topic: topic}) do

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

        if valid_topic do
            true
        else
            false
        end
    end


    defp create_subscription(%Broker{}=broker, pid, opts) do

        %Broker{handle: handle, position: hpos} = broker

        ack = 
            case {Keyword.get(opts, :start, :current), hpos} do
                {:current, 0} ->
                    Signal.Store.Adapter.get_cursor(broker.app)

                {:genesis, 0} ->
                    0

                {position, _} when is_number(position) ->
                    position

                {_, hpos} ->
                    hpos
            end
        track = Keyword.get(opts, :track, true)
        topics = Keyword.get(opts, :topics, []) |> List.wrap()
        streams = Keyword.get(opts, :streams, []) |> List.wrap()
        %{
            id: pid,
            ack: ack,
            syn: ack,
            track: track,
            handle: handle,
            topics: topics,
            streams: streams,
        }
    end

    defp handle_ack(%Broker{}=broker, pid, number) do
        %Broker{subscriptions: subscriptions, buffer: buffer} = broker
        ack_sub = &(Map.get(&1, :id) == pid and Map.get(&1, :syn) == number)
        index = Enum.find_index(subscriptions, ack_sub)

        if is_nil(index) do
            broker
        else

            subscriptions = 
                List.update_at(subscriptions, index, fn subscription -> 
                    info = """
                    [BROKER] #{broker.handle}
                    acknowledged: #{number}
                    buffer: #{length(buffer)}
                    """
                    Logger.info(info)
                    Map.put(subscription, :ack, number)
                end)

            %{ack: max_ack} = 
                subscriptions
                |> Enum.max_by(&(Map.get(&1, :ack)), fn -> %{ack: number} end)

            buffer = Enum.filter(buffer, &(Map.get(&1, :number) > max_ack))

            %Broker{broker | subscriptions: subscriptions, buffer: buffer, ready: true}
            |> sched_next()
        end
    end

    def start_worker_stream(%Broker{worker: worker}=broker) do
        broker_pid = self()

        if not(is_nil(worker)) do
            send(worker.pid, :stop)
        end

        config = [
            range: [broker.position + 1],
            topics: broker.topics, 
            streams: broker.streams,
        ] 

        worker =
            Task.async(fn -> 
                broker.app
                |> Signal.Store.Adapter.read_events(fn event -> 
                    send(broker_pid, {:push, event})
                    receive do
                        :stop ->
                            :stoped

                        :continue ->
                            nil
                    end
                end, config)
                {:worker, {:done, self()}}
            end)
        %Broker{broker| worker: worker}
    end

    def sched_next(%Broker{buffer: [], worker: nil}=broker) do
        broker
    end

    def sched_next(%Broker{buffer: [], ready: ready}=broker) do
        %Broker{worker: worker}=broker
        if ready do
            send(worker.pid, :continue)
        end
        broker
    end

    def sched_next(%Broker{buffer: _buffer, ready: false}=broker) do
        broker
    end

    def sched_next(%Broker{buffer: [event | buffer], subscriptions: subs}=broker) do

        nil_max = fn -> nil end
        max_sub = Enum.max_by(subs, &(Map.get(&1, :syn)), nil_max)

        if is_nil(max_sub) do
            broker
        else
            if max_sub.ack == max_sub.syn do
                send(self(), {:push, event})
                %Broker{broker | buffer: buffer, ready: false}
            else
                broker
            end
        end
    end

    def collect_streams_and_topics(subscriptions) do
        Enum.reduce(subscriptions, {[],[]}, fn 
            %{streams: sub_streams, topics: sub_topics}, {streams, topics} -> 
                stream_ids = for {stream_id, _type} <- sub_streams, do: stream_id
                streams = Enum.uniq(streams ++ stream_ids)
                topics = Enum.uniq(topics ++ sub_topics)
                {streams, topics}
        end)
    end

    def subscribe(app, handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(app, Atom.to_string(handle), opts)
    end

    def subscribe(app, handle, opts) when is_list(opts) and is_binary(handle) do
        track = Keyword.get(opts, :track, true)
        app
        |> Supervisor.prepare_broker(handle, track)
        |> GenServer.call({:subscribe, opts}, 5000)
    end

    def unsubscribe(app, handle, _opts\\[]) do
        broker = Supervisor.broker(app, handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, :unsubscribe, 5000)
        end
    end

    def subscription(app, handle) when is_binary(handle) do
        broker = Supervisor.broker(app, handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, :subscription, 5000)
        end
    end

    def acknowledge(app, handle, number) do
        broker = Supervisor.broker(app, handle)
        with {:via, _reg, _iden} <- broker do
            GenServer.call(broker, {:ack, self(), number})
        end
    end

end


