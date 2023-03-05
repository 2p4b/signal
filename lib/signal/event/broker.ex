defmodule Signal.Event.Broker do
    use GenServer, restart: :transient
    alias Signal.Event
    alias Signal.Tracker
    alias Signal.PubSub
    alias Signal.Event.Broker
    alias Signal.Store.Helper
    alias Signal.Store.Adapter

    defstruct [
        app: nil,
        ack: nil,
        uuid: nil,
        handle: nil,
        worker: nil,
        consumer: nil,
        ready: true,
        pushed: [],
        cursor: 0, 
        buffer: [],
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
        opts = 
            [uuid: UUID.uuid4()]
            |> Keyword.merge(opts)

        Signal.PubSub.subscribe(app, handle)
        {:ok, struct(__MODULE__, opts), {:continue, :load_consumer}}
    end

    @impl true
    def handle_continue(:load_consumer, %Broker{}=broker) do
        params = %{
            app: broker.app, 
            uuid: broker.uuid,
            broker: true, 
            handle: broker.handle, 
            ts: DateTime.utc_now(), 
        }
        {:ok, _ref} = Tracker.track(broker.app, "broker", broker.handle, params)

        consumers = list_consumers(broker.app, broker.handle)

        case consumers do
            [] ->
                # Stop broker if there are 
                # no consumers
                {:stop, :normal, broker}
            [consumer| _] ->
                broker = %Broker{broker| 
                    ack: consumer.ack,
                    consumer: consumer
                }
                {:noreply, broker, {:continue, :start}} 
        end
    end

    @impl true
    def handle_continue(:start, %Broker{}=broker) do
        {:noreply, start_worker_stream(broker)}
    end

    @impl true
    def handle_call({:state, nil}, _from, %Broker{}=broker) do
        {:reply, broker, broker} 
    end

    @impl true
    def handle_call({:state, prop}, _from, %Broker{}=broker) do
        {:reply, Map.get(broker, prop), broker} 
    end

    @impl true
    def handle_cast({:unsubscribe, _uuid}, %Broker{}=broker) do
        case list_consumers(broker) do
            [] ->
                {:stop, :normal, broker}

            _ ->
                {:noreply, broker, {:continue, :load_consumer}} 
        end
    end

    @impl true
    def handle_info({:ack, id, number}, %Broker{consumer: %{uuid: cuuid, pushed: [%Event{number: syn}|_]}}=broker) 
    when id === cuuid and  number === syn do

        %Broker{handle: handle, ack: b_ack} = broker

        broker = handle_consumer_ack(broker, number)

        %{ack: ack, track: track} = broker.consumer

        if track and (ack > b_ack) do
            {:ok, ^number} = 
                Adapter.handler_acknowledge(broker.app, handle, number)

            [
                app: broker.app,
                handle: broker.handle,
                consumer: broker.consumer.uuid,
                ack: number
            ]
            |> Signal.Logger.info(label: :broker)

            {:noreply, broker}
        else
            {:noreply,  broker}
        end
    end

    @impl true
    def handle_info({:ack, _uuid, _number}, %Broker{}=broker) do
        {:noreply, broker}
    end

    @impl true
    def handle_info({:push, _}, %Broker{consumer: nil}=broker) do
        {:noreply, broker}
    end

    @impl true
    def handle_info({:push, %Event{number: number}=event}, %Broker{}=broker) do

        %Broker{consumer: consumer, pushed: pushed, ack: ack} = broker

        last = List.last(pushed)
        if number > ack and (is_nil(last) or (number > last.number)) do
            [
                app: broker.app,
                handle: broker.handle,
                pushed: event.topic,
                number: event.number
            ]
            |> Signal.Logger.info(label: :broker)

            broker.app
            |> Signal.PubSub.broadcast(consumer.uuid, event)

            pushed = pushed ++ List.wrap(event)

            ready = length(pushed) < consumer.buffer_size

            broker = 
                %Broker{broker|pushed: pushed, ready: ready}
                |> sched_next()

            {:noreply, broker}
        else
            broker = 
                broker
                |> struct(%{ready: true}) 
                |> sched_next()

            {:noreply, broker}
        end
    end

    @impl true
    def handle_info(%Event{}=event, %Broker{buffer: buffer, worker: nil}=broker) do
        %Broker{consumer: %{streams: streams, topics: topics}}=broker
        if Helper.event_is_valid?(event, streams, topics) do
            broker = 
                %Broker{broker | 
                    buffer: buffer ++ List.wrap(event)
                }
                |> sched_next()

            {:noreply, broker}
        else
            {:noreply, broker}
        end
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
        if is_nil(broker.consumer) do
            {:noreply, %Broker{broker| worker: nil, buffer: []}}
        else
            broker.app
            |> PubSub.subscribe_to_events()

            # Pull events thats may have fallen through
            config = [
                range: [broker.ack + 1],
                topics: broker.consumer.topics, 
                streams: broker.consumer.streams,
            ] 

            fallen = 
                broker.app
                |> Adapter.list_events(config)

            broker = %Broker{broker | buffer: fallen, worker: nil}
            {:noreply, sched_next(broker)}
        end
    end

    @impl true
    def handle_info({:DOWN, _ref, :process, _pid, :normal}, %Broker{}=broker) do
        {:noreply, broker}
    end

    defp handle_consumer_ack(%Broker{consumer: %{syn: syn}}=broker, number) 
    when syn === number do
        %Broker{consumer: consumer, pushed: pushed,  buffer: buffer} = broker

        buffer = Enum.filter(buffer, &(Map.get(&1, :number) > number))
        pushed = Enum.filter(pushed, &(Map.get(&1, :number) > number))

        ready = length(pushed) < consumer.buffer_size

        %Broker{broker | 
            ack: number,
            ready: ready,
            buffer: buffer, 
            pushed: pushed,
            consumer: Map.put(consumer, :ack, number)
        }
        |> sched_next()
    end

    def start_worker_stream(%Broker{worker: worker, consumer: consumer}=broker) do
        broker_pid = self()

        if not(is_nil(worker)) do
            send(worker.pid, :stop)
        end

        config = [
            range: [broker.ack + 1],
            topics: consumer.topics, 
            streams: consumer.streams,
        ] 

        worker =
            Task.async(fn -> 
                broker.app
                |> Adapter.read_events(fn event -> 
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
        %Broker{broker| buffer: [], worker: worker}
    end

    def sched_next(%Broker{consumer: nil}=broker) do
        broker
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

    def sched_next(%Broker{buffer: [event | buffer]}=broker) do
        send(self(), {:push, event})
        %Broker{broker | buffer: buffer, ready: false}
    end

    def subscribe(app, handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(app, Atom.to_string(handle), opts)
    end

    def subscribe(app, handle, opts) when is_list(opts) and is_binary(handle) do
        uuid = Keyword.get(opts, :uuid, UUID.uuid4())
        track = Keyword.get(opts, :track, true)
        topics = Keyword.get(opts, :topics, [])
        streams = Keyword.get(opts, :streams, [])
        buffer_size = Keyword.get(opts, :buffer, 20)
        ack = 
            case Adapter.handler_position(app, handle) do
                nil ->
                    value = Keyword.get(opts, :start)
                    cond do
                        is_nil(value) or value in [:cursor] ->
                            Signal.Store.Adapter.get_cursor(app)

                        value in [:beginning]
                            0

                        is_integer(value) and value >= 0 ->
                            value

                        true ->
                            message = """
                            invalid consumer start value
                                handle: #{handle}
                                opts: #{inspect(opts)}

                                valid start: 
                                    :cursor, :beginning, non_negative_integer() 

                                    :cursor
                                        if the handle does not exist start processing
                                        events from the current store cursor position

                                    :beginning
                                        if the handle does not exist start processing
                                        events from the beginning or 0

                                    non_negative_integer()
                                        if the handle does not exists start processing
                                        events from non_negative_integer() value
                            """
                            raise(ArgumentError, [message: message])
                    end

                value -> 
                    value
            end

        params = %{
            app: app,
            ack: ack,
            uuid: uuid,
            node: Node.self(),
            track: track,
            handle: handle,
            topics: topics,
            streams: streams,
            consumer: true,
            buffer_size: buffer_size,
            ts: DateTime.utc_now(),
        }
        Signal.PubSub.subscribe(app, uuid)
        {:ok, _ref} = Tracker.track(app, "consumer", handle, params)
        params
    end

    def unsubscribe(app, consumer, _opts\\[]) do
        Tracker.untrack(app, "consumer", consumer.handle)
        Signal.PubSub.unsubscribe(app, consumer.uuid)
    end

    def acknowledge(app, consumer, number) do
        Signal.PubSub.broadcast(app, consumer.handle, {:ack, consumer.uuid, number})
    end

    def list_consumers(%Broker{app: app, handle: handle}) do
        list_consumers(app, handle)
    end

    def list_consumers(app, handle) do
        selector = &(elem(&1,0) === handle && elem(&1,1).node === Node.self())
        app
        |> Tracker.list("consumer")
        |> Enum.filter(selector)
        |> Enum.map(&(elem(&1, 1)))
    end

    @impl true
    def terminate(reason, broker) do
        [
            app: broker.app,
            handle: broker.handle,
            status: :terminated,
            reason: reason,
            ack: broker.ack
        ]
        |> Signal.Logger.info(label: :broker)
    end

end


