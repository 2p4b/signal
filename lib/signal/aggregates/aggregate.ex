defmodule Signal.Aggregates.Aggregate do
    use GenServer

    alias Signal.Codec
    alias Signal.Events.Event
    alias Signal.Stream.Broker
    alias Signal.Stream.Reducer
    alias Signal.Aggregates.Aggregate

    defstruct [
        :app, 
        :store,
        :state, 
        :stream, 
        index: 0, 
        version: 0,
        awaiting: [],
        snapshot: {nil, nil},
    ]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        Process.send(self(), :init, [])
        {:ok, struct(__MODULE__, opts )}
    end

    @impl true
    def handle_call(:state, _from, %Aggregate{state: state}=aggregate) do
        {:reply, state, aggregate} 
    end

    @impl true
    def handle_call({:state, _red}, _from, %Aggregate{state: state}=aggregate) do
        {:reply, state, aggregate} 
    end

    @impl true
    def handle_call({:await, red}, from, %Aggregate{version: ver, awaiting: waiting, state: state}=aggregate) do
        if ver >= red do
            {:reply, state, aggregate} 
        else
            ref = Process.monitor(elem(from, 0))
            {:noreply, %Aggregate{aggregate | awaiting: waiting ++ [{from, ref, red}]} }
        end
    end

    @impl true
    def handle_info(%Event{number: number}=event, %Aggregate{app: app, stream: stream}=aggregate) do
        case apply_event(aggregate, event) do
            %Aggregate{} = aggregate ->
                Broker.acknowledge(app, stream, number)
                {:noreply, reply_waiters(aggregate)} 

            error ->
                {:stop, error, aggregate}
        end
    end

    @impl true
    def handle_info(:init, %Aggregate{app: app, store: store}=aggregate) do
        aggregate = 
            case store.get_state(app, aggregate_id(aggregate), :max) do
                nil -> aggregate
                {_index, _data} = snapshot ->
                    load(aggregate, snapshot)
            end
        listen(aggregate)
        {:noreply, aggregate}
    end

    @impl true
    def handle_info(:timeout, %Aggregate{} = state) do
        {:stop, :normal, state}
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _obj, _reason}, %Aggregate{}=aggregate) do
        awaiting =
            aggregate.awaiting
            |> Enum.filter(fn {_pid, wref, _stage} -> 
                if ref == wref do
                    Process.demonitor(wref)
                    false
                else
                    true
                end
            end)
        {:noreply, %Aggregate{aggregate | awaiting: awaiting}}
    end

    defp reply_waiters(%Aggregate{version: ver, state: state, awaiting: waiters}=aggregate) do
        awaiting =
            Enum.filter(waiters, fn {from, ref, stage} -> 
                if ver >= stage do
                    Process.demonitor(ref)
                    GenServer.reply(from, state)
                    false
                else
                    true
                end
            end)

        %Aggregate{aggregate | awaiting: awaiting}
    end

    def state(aggregate, timeout \\ 5000) do
        GenServer.call(aggregate, :state, timeout)
    end

    defp apply_event(%Aggregate{}=aggregate, %Event{number: number}=event) do
        %Aggregate{version: version, state: state} = aggregate

        case event do
            %Event{reduction: reduction, payload: payload} when reduction > version ->

                state = Reducer.apply(state, Event.meta(event), payload)

                %Aggregate{aggregate | 
                    state: state,
                    index: number,
                    version: reduction
                }
                |> snapshot()

            _ -> {:error, :out_of_order, event}
        end
    end

    defp apply_event(%Aggregate{}, event) do
        {:error, {:invalid_event, event}}
    end

    def apply(aggregate, %Event{}=event) when is_pid(aggregate) do
        Process.send(aggregate, event, [])
    end

    def apply(aggregate, %Event{}=event) do
        GenServer.whereis(aggregate) |> Aggregate.apply(event)
    end

    def await(aggregate, stage, timeout \\ 5000) do
        GenServer.call(aggregate, {:await, stage}, timeout)
    end

    defp aggregate_id(%Aggregate{stream: stream}) do
        aggregate_id(stream)
    end

    defp aggregate_id({_type, id}) do
        "aggregate:" <> id
    end

    defp snapshot(%Aggregate{app: app, version: version, store: store}=aggregate) do
        id = aggregate_id(aggregate)
        data = encode(aggregate)
        {:ok, ^version} = Kernel.apply(store, :set_state, [app, id, version, data])
        aggregate
    end

    def encode(%Aggregate{ state: state, index: index}) do
        %{index: index, data: Codec.encode(state)}
    end

    def load(%Aggregate{state: state}=aggr, {version, %{data: data, index: index}}) do
        %Aggregate{aggr | 
            state: Codec.load(state, data), 
            index: index,
            version: version, 
        }
    end

    def listen(%Aggregate{app: app, stream: stream, index: index}) do
        Broker.stream_from(app, stream, index)
    end

end
