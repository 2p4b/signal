defmodule Signal.Stream.Broker do

    use GenServer
    alias Signal.Events.Event
    alias Signal.Stream.Broker
    alias Signal.Stream.Consumer

    defstruct [:type, :consumers, :index, :store, :app]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        app = Keyword.get(opts, :app)
        type = Keyword.get(opts, :type)
        store = Keyword.get(opts, :store)
        Process.send(self(), :init, [])
        args = [
            index: 0,
            app: app,
            type: type,
            store: store,
            consumers: [],
        ]
        {:ok, struct(__MODULE__, args)}
    end

    @impl true
    def handle_info(:init, %Broker{app: app}=broker) do
        Signal.Application.listen(app)
        {:noreply, broker} 
    end

    @impl true
    def handle_info({:next, pid}, broker) do
        {:noreply, handle_next(pid, broker)} 
    end

    @impl true
    def handle_info(%Event{stream: {stype, _sid}}=event, %Broker{type: type}=broker) do
        if stype == type do 
            {:noreply, handle_event(event, broker)} 
        else
            {:noreply, broker} 
        end
    end

    @impl true
    def handle_info({:ack, pid, number}, %Broker{}= broker) do
        {:noreply, handle_ack(pid, number, broker)} 
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _obj, _reason}, %Broker{}= broker) do
        consumers = 
            broker.consumers
            |> Enum.filter(fn %Consumer{ref: sref} -> 
                sref != ref 
            end)
        {:noreply, %Broker{broker| consumers: consumers}}
    end

    @impl true
    def handle_call(:state, _from, broker) do
        {:reply, broker, broker}
    end

    @impl true
    def handle_call({:stream, id, ack}, {pid, _ref}, %Broker{}=broker) do
        {%Consumer{}=consumer, broker} = create_consumer(pid, id, ack, broker)
        {:reply, consumer, sched_next(consumer, broker)} 
    end

    defp sched_next(%Consumer{pid: pid, ack: ack, syn: syn}, %Broker{}=broker) 
    when syn == ack do
        Process.send(self(), {:next, pid}, [])
        broker
    end

    defp sched_next(%Consumer{}, %Broker{}=broker) do
        broker
    end

    defp create_consumer(pid, id, ack, %Broker{}=broker) 
    when is_pid(pid) and is_binary(id) and is_integer(ack) do

        consumer = get_consumer(pid, broker)

        {consumer, consumers} = 
            if is_nil(consumer) do
                consumer = %Consumer{ 
                    pid: pid,
                    ack: ack,
                    syn: ack,
                    ref: Process.monitor(pid),
                    stream: {broker.type, id},
                }
                {consumer, broker.consumers ++ [consumer]}
            else
                {consumer, broker.consumers}
            end

        {consumer, %Broker{broker | consumers: consumers}}
    end

    defp handle_event(%Event{number: number, stream: stream}, %Broker{}=broker) do
        broker.consumers
        |> Enum.filter(fn %Consumer{ack: ack, syn: syn, stream: cstream}-> 
            cstream == stream and ack == syn
        end)
        |> Enum.reduce(%Broker{broker| index: number}, fn consumer, broker -> 
            sched_next(consumer, broker)
        end)
    end

    defp handle_next(pid,  %Broker{}=broker) when is_pid(pid) do
        case get_consumer(pid, broker) do
            %Consumer{ack: ack, syn: syn, pid: pid}=consumer when ack == syn ->
                case pull_event(consumer, broker) do
                    %Event{number: number}=event ->
                        Process.send(pid, event, [])
                        update_consumer(%Consumer{consumer | syn: number}, broker)
                    _ ->
                        broker
                end

            _ -> broker
        end
    end

    defp handle_ack(pid, ack, %Broker{}=broker) when is_pid(pid) and is_integer(ack) do
        case get_consumer(pid, broker) do
            %Consumer{}=consumer->
                consumer = %Consumer{consumer | ack: ack}
                sched_next(consumer, update_consumer(consumer, broker))
                
            _ -> broker
        end
    end

    defp update_consumer(%Consumer{pid: pid}=consumer, %Broker{}=broker) do
        consumers =
            broker.consumers
            |> Enum.map(fn 
                %Consumer{pid: ^pid} -> 
                    consumer
                %Consumer{}=consumer ->
                    consumer
            end)
        %Broker{broker | consumers: consumers}
    end

    defp get_consumer(pid, %Broker{consumers: consumers}) when is_pid(pid) do
        Enum.find(consumers, &(Map.get(&1, :pid) == pid))
    end

    defp pull_event(%Consumer{stream: stream, ack: ack}, %Broker{app: app, store: store}) do
        opts = [stream: stream]
        store.next(app, ack, opts) 
    end

    def stream_from(app, {type, id}, position \\ 0) when is_integer(position) and is_atom(type) and is_binary(id) do
        app
        |> Signal.Stream.Supervisor.prepare_broker(type)
        |> GenServer.call({:stream, id, position} , 5000)
    end

    def acknowledge(app, {type, _id}, number) 
    when is_atom(type) and is_integer(number) do
        app
        |> Signal.Stream.Supervisor.prepare_broker(type)
        |> GenServer.whereis()
        |> Process.send({:ack, self(), number}, [])
    end

end
