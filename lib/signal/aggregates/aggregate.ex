defmodule Signal.Aggregates.Aggregate do
    use GenServer, restart: :transient
    alias Signal.Codec
    alias Signal.Timer
    alias Signal.Event
    alias Signal.Snapshot
    alias Signal.Stream.Reducer
    alias Signal.Aggregates.Aggregate
    require Logger

    defstruct [
        :id,
        :app, 
        :store,
        :state, 
        :timeout,
        :stream, 
        :subscription,
        ack: 0, 
        version: 0,
        awaiting: [],
    ]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        aggregate_opts = [
            name: Keyword.get(opts, :name), 
            hibernate_after: Timer.min(60)
        ]
        GenServer.start_link(__MODULE__, opts, aggregate_opts)
    end

    @impl true
    def init(opts) do
        Process.send(self(), :init, [])
        {:ok, struct(__MODULE__, opts), Timer.hours(1)}
    end

    @impl true
    def handle_call({:state, opts}, from, %Aggregate{}=aggregate) do
        %Aggregate{
            state: state,
            version: ver, 
            timeout: timeout,
            awaiting: waiting, 
        } = aggregate

        red = Keyword.get(opts, :version, aggregate.version)
        if ver >= red do
            {:reply, state, aggregate, timeout} 
        else
            ref = Process.monitor(elem(from, 0))
            aggregate = %Aggregate{aggregate | 
                awaiting: waiting ++ [{from, ref, red}]
            }
            {:noreply, aggregate, timeout}
        end
    end

    @impl true
    def handle_call({:revise, {version, state}}, _from, %Aggregate{}=aggregate) do
        %Aggregate{version: ver, timeout: timeout} = aggregate

        %Aggregate{} = 
            %Aggregate{aggregate | version: version, state: state} 
            |> snapshot()

        aggregate = 
            if version == ver do
                %Aggregate{aggregate| state: state}
            else
                aggregate
            end

        {:reply, {:ok, {version, state}}, aggregate, timeout} 
    end

    @impl true
    def handle_call({:await, red}, from, %Aggregate{}=aggregate) do
        %Aggregate{
            state: state,
            version: vsn, 
            awaiting: waiting, 
        } = aggregate

        if vsn >= red do
            {:reply, state, aggregate, aggregate.timeout} 
        else
            ref = Process.monitor(elem(from, 0))
            {:noreply, %Aggregate{aggregate | awaiting: waiting ++ [{from, ref, red}]}, aggregate.timeout }
        end
    end

    @impl true
    def handle_info(:init, %Aggregate{}=aggregate) do
        %Aggregate{
            app: {application, _tenant}, 
            stream: {stream_id, _}
        } = aggregate

        record = 
            application
            |> Signal.Store.Adapter.get_snapshot(stream_id)

        aggregate = 
            if is_nil(record) do
                aggregate
            else
                load(aggregate, record)
            end

        {:noreply, listen(aggregate), aggregate.timeout}
    end

    @impl true
    def handle_info(%Event{number: number}, %Aggregate{ack: ack}=aggregate) 
    when number <= ack do
        {:noreply, aggregate, aggregate.timeout}
    end

    @impl true
    def handle_info(%Event{}=event, %Aggregate{}=aggregate) do
        case apply_event(aggregate, event) do
            {:ok, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                {:noreply, aggregate, aggregate.timeout} 

            {:hibernate, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                {:noreply, aggregate, :hibernate} 

            {:stop, reason, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                {:stop, reason, aggregate} 

            error ->
                {:stop, error, aggregate}
        end
    end

    @impl true
    def handle_info(:timeout, %Aggregate{}=aggregate) do
        case aggregate.awaiting  do
            [] ->
                app = aggregate.app
                name  = Signal.Aggregates.Supervisor.process_name(aggregate.stream)
                Signal.Aggregates.Supervisor.unregister_child(app, name)
                {:stop, :normal, aggregate}
            _ ->
                # NO Sleep if there are waiters
                {:noreply, aggregate, aggregate.timeout}
        end
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
        {:noreply, %Aggregate{aggregate | awaiting: awaiting}, aggregate.timeout}
    end

    defp reply_waiters(%Aggregate{}=aggregate) do
        %Aggregate{
            version: vsn, 
            state: state, 
            awaiting: waiters
        } = aggregate

        awaiting =
            Enum.filter(waiters, fn {from, ref, stage} -> 
                if vsn >= stage do
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
        timeout = Keyword.get(opts, :timeout, 10000)
        GenServer.call(aggregate, {:state, opts}, timeout)
    end

    def revise(aggregate, {version, state},  opts \\ []) do
        timeout = Keyword.get(opts, :timeout, 10000)
        GenServer.call(aggregate, {:revise, {version, state}}, timeout)
    end

    defp apply_event(%Aggregate{}=aggregate, %Event{number: number}=event) do
        %Aggregate{
            version: version, 
            stream: {_, stream_type},
            state: state, 
        } = aggregate

        case event do
            %Event{index: index} when index == (version + 1) ->

                metadata = Event.metadata(event)
                
                event_payload = Event.payload(event)

                info = """
                applying: #{event.topic}
                index:    #{event.index}
                number: #{event.number}
                """
                log(info, aggregate)

                apply_args = 
                    case Reducer.impl_for(event_payload) do
                        nil ->
                            [state, metadata, event_payload]

                        _impl ->
                            [event_payload, metadata, state]
                    end

                case Kernel.apply(Reducer, :apply, apply_args) do
                    %{__struct__: type}=state when is_atom(type) and type == stream_type ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: index
                            }
                        {:ok, aggregate}

                    {:ok, state}  ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: index
                            }
                        {:ok, aggregate}

                    {:ok, state, timeout} when is_number(timeout)  ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                timeout: timeout,
                                version: index
                            }
                        {:ok, aggregate}


                    {:snapshot, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: index
                            }
                            |> snapshot()

                        {:ok, aggregate}

                    {:snapshot, state, :sleep} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: index
                            }
                            |> snapshot()

                        {:hibernate, aggregate}

                    {:snapshot, state, timeout} when is_number(timeout) ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                timeout: timeout,
                                version: index
                            }
                            |> snapshot()

                        {:ok, aggregate}

                    {:sleep, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: index
                            }

                        {:hibernate, aggregate}

                    {:sleep, state, timeout} when is_number(timeout) ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                timeout: timeout,
                                version: index
                            }

                        {:hibernate, aggregate}

                    {:error, error} ->
                        {:error, error}

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
            subscription: %{handle: handle}
        } = aggregate

        {application, _tenant} = app

        application
        |> Signal.Store.Adapter.handler_acknowledge(handle, number)

        "acknowleded: #{number}"
        |> log(aggregate)

        aggregate
    end

    defp snapshot(%Aggregate{}=aggregate) do
        %Aggregate{
            app: app, 
            stream: stream,
            version: version
        } = aggregate

        {application, _tenant} = app
        {stream_id, _type} = stream
        payload = encode(aggregate)

        snapshot = 
            stream_id
            |> Snapshot.new(payload, version: version)

        Signal.Store.Adapter.record_snapshot(application, snapshot)

        "snapshot: #{version}"
        |> log(aggregate)

        aggregate
    end

    def load(%Aggregate{state: state}=aggr, %Snapshot{}=snapshot) do
        %Snapshot{version: version, payload: payload}=snapshot
        case payload do
            %{"index" => index, "state" => payload} ->
                {:ok, aggregate_state} = Codec.load(state, payload)
                %Aggregate{aggr | 
                    ack: index,
                    state: aggregate_state, 
                    version: version, 
                }

            _ ->
                aggr
        end
    end

    def encode(%Aggregate{ack: index, state: state}) do
        {:ok, data} = Codec.encode(state)
        %{"index" => index, "state" => data}
    end

    defp listen(%Aggregate{app: app, stream: stream, ack: ack}=aggr) do
        {application, _tenant} = app
        {_id, stream_type} = stream
        opts = [
            start: ack,
            track: false, 
            streams: stream |> List.wrap(), 
        ]
        {:ok, subscription} = 
            application
            |> Signal.Event.Broker.subscribe(stream_type, opts)
        %Aggregate{aggr| subscription: subscription}
    end

    @impl true
    def terminate(reason, %Aggregate{}=aggregate) do
        "shutdown: #{inspect(reason)}"
        |> log(aggregate)
        reason
    end

    def log(info, %Aggregate{}=aggregate) do
        %Aggregate{
            version: version, 
            state: %{__struct__: state_type}, 
            stream: {stream_id, _stream_type} 
        } = aggregate

        text = """

        [AGGREGATE] #{state_type} 
        stream: #{stream_id}
        version: #{version}
        #{info}
        """
        Logger.info(text)
    end

end
