defmodule Signal.Aggregates.Aggregate do
    use GenServer
    alias Signal.Codec
    alias Signal.Snapshot
    alias Signal.Stream.Event
    alias Signal.Stream.Reducer
    alias Signal.Aggregates.Aggregate

    defstruct [
        :app, 
        :store,
        :state, 
        :stream, 
        :subscription,
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
    def handle_info(%Event{}=event, %Aggregate{}=aggregate) do
        case apply_event(aggregate, event) do
            %Aggregate{} = aggregate ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                {:noreply, aggregate} 

            error ->
                {:stop, error, aggregate}
        end
    end

    @impl true
    def handle_info(:init, %Aggregate{}=aggregate) do
        %Aggregate{app: {app_module, _tenant}, stream: stream}=aggregate
        aggregate = 
            case app_module.snapshot(aggregate_id(stream)) do
                nil -> 
                    aggregate

                snapshot ->
                    load(aggregate, snapshot)
            end
        {:noreply, listen(aggregate)}
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
            %Event{position: position} when position == (version + 1) ->

                metadata = Event.metadata(event)
                
                event_payload = Event.payload(event)

                case Reducer.apply(state, metadata, event_payload) do
                    {:snapshot, state} ->
                        %Aggregate{aggregate | 
                            state: state,
                            index: number,
                            version: position
                        }
                        |> snapshot()

                    state ->
                        %Aggregate{aggregate | 
                            state: state,
                            index: number,
                            version: position
                        }
                end


            _ -> 
                IO.inspect("expected version #{version+1} got #{event.position}")
                {:error, :out_of_order, event}
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

    def aggregate_id(%Aggregate{stream: stream}) do
        aggregate_id(stream)
    end

    def aggregate_id({type, id}) when is_atom(type) do
        Signal.Helper.module_to_string(type)
        |> aggregate_id(id)
    end

    def aggregate_id(type, id) when is_binary(type) do
        type <> ":" <> id
    end

    defp acknowledge(%Aggregate{}=aggregate, %Event{number: number}) do
        %Aggregate{app: app, subscription: %{handle: handle}}=aggregate
        {app_module, _tenant} = app
        app_module.acknowledge(handle, number, [])
        aggregate
    end

    defp snapshot(%Aggregate{app: app, version: version, stream: stream}=aggregate) do
        {application, _tenant} = app
        snapshot = %Snapshot{
            id: aggregate_id(stream),
            data: encode(aggregate),
            version: version,
        }
        application.record(snapshot)
        aggregate
    end

    def encode(%Aggregate{ state: state}) do
        Codec.encode(state)
    end

    def load(%Aggregate{state: state}=aggr, %Snapshot{}=snapshot) do
        %Snapshot{version: version, data: data}=snapshot
        %Aggregate{aggr | 
            version: version, 
            state: Codec.load(state, data), 
        }
    end

    def listen(%Aggregate{app: app, stream: stream, version: version}=aggr) do
        {application, _tenant} = app
        {:ok, subscription} = application.subscribe([stream: stream, position: version])
        %Aggregate{aggr| subscription: subscription}
    end

end
