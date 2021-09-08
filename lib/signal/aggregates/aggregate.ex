defmodule Signal.Aggregates.Aggregate do
    use GenServer
    alias Signal.Codec
    alias Signal.Snapshot
    alias Signal.Stream.Event
    alias Signal.Stream.Reducer
    alias Signal.Aggregates.Aggregate
    require Logger

    defstruct [
        :id,
        :app, 
        :store,
        :state, 
        :stream, 
        :subscription,
        index: 0, 
        version: 0,
        awaiting: [],
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
    def handle_call({:state, opts}, from, %Aggregate{}=aggregate) do
        %Aggregate{version: ver, awaiting: waiting, state: state}=aggregate
        red = Keyword.get(opts, :version, aggregate.version)
        if ver >= red do
            {:reply, state, aggregate} 
        else
            ref = Process.monitor(elem(from, 0))
            {:noreply, %Aggregate{aggregate | awaiting: waiting ++ [{from, ref, red}]} }
        end
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
    def handle_info(:init, %Aggregate{}=aggregate) do
        %Aggregate{app: {application, tenant}, stream: {_, stream}}=aggregate
        aggregate = 
            case application.snapshot(stream, tenant: tenant) do
                nil -> 
                    aggregate

                snapshot ->
                    load(aggregate, snapshot)
            end
        {:noreply, listen(aggregate)}
    end

    @impl true
    def handle_info(%Event{number: number}, %Aggregate{index: index}=aggregate) 
    when number <= index do
        {:noreply, aggregate}
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

    def state(aggregate,  opts \\ []) do
        timeout = Keyword.get(opts, :timeout, 5000)
        GenServer.call(aggregate, {:state, opts}, timeout)
    end

    defp apply_event(%Aggregate{}=aggregate, %Event{number: number}=event) do
        %Aggregate{version: version, state: state, stream: {_, stream} } = aggregate
        case event do
            %Event{position: position} when position == (version + 1) ->

                metadata = Event.metadata(event)
                
                event_payload = Event.payload(event)

                info = """
                [Aggregate] #{state.__struct__} 
                stream: #{stream}
                applying: #{event.type}
                version: #{event.position}
                """
                Logger.info(info)

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

    defp acknowledge(%Aggregate{}=aggregate, %Event{number: number}) do
        %Aggregate{
            app: app, 
            state: state,
            stream: {_, stream}, 
            subscription: %{handle: handle}
        } = aggregate

        {application, tenant} = app
        application.acknowledge(handle, number, [tenant: tenant])
        info = """
        [Aggregate] #{state.__struct__} 
        stream: #{stream}
        acknowleded: #{number}
        version: #{aggregate.version}
        """
        Logger.info(info)
        aggregate
    end

    defp snapshot(%Aggregate{app: app, version: version, stream: stream}=aggregate) do
        {application, _tenant} = app
        {_, id} = stream
        snapshot = %Snapshot{
            id: id,
            data: encode(aggregate),
            version: version,
        }
        application.record(snapshot)
        aggregate
    end

    def load(%Aggregate{state: state}=aggr, %Snapshot{}=snapshot) do
        %Snapshot{version: version, data: data}=snapshot
        case data do
            %{index: index, state: payload} ->
                %Aggregate{aggr | 
                    index: index,
                    version: version, 
                    state: Codec.load(state, payload), 
                }

            _ ->
                aggr
        end
    end

    def encode(%Aggregate{index: index, state: state}) do
        %{
            index: index,
            state: Codec.encode(state)
        }
    end

    def listen(%Aggregate{app: app, stream: {_, stream}, index: index}=aggr) do
        {application, _tenant} = app
        {:ok, subscription} = application.subscribe([
            from: index,
            track: false, 
            stream: stream, 
        ])
        %Aggregate{aggr| subscription: subscription}
    end

end
