defmodule Signal.Aggregates.Aggregate do
    use GenServer, restart: :transient
    use Signal.Telemetry
    alias Signal.Codec
    alias Signal.Timer
    alias Signal.Event
    alias Signal.Snapshot
    alias Signal.Stream.Reducer
    alias Signal.Aggregates.Aggregate

    defmodule Waiter do
        defstruct [
            :ref,
            :vsn,
            :from,
            :start,
            :timeout
        ]
    end

    defstruct [
        :id,
        :app, 
        :ref,
        :store,
        :state, 
        :timeout,
        :stream, 
        :consumer,
        :module,
        ack: 0, 
        wait: 5000,
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
        aggregate = struct(__MODULE__, opts)
        telemetry_start(:init, metadata(aggregate), measurements(aggregate))
        {:ok, aggregate, {:continue, :load_aggregate}}
    end

    @impl true
    def handle_continue(:load_aggregate, %Aggregate{}=aggregate) do
        start = telemetry_start(:load, metadata(aggregate), %{})
        %Aggregate{
            app: app,
            stream: {stream_id, _}
        } = aggregate

        record = 
            app
            |> Signal.Store.Adapter.get_snapshot(stream_id)

        aggregate = 
            if is_nil(record) do
                aggregate
            else
                load_state(aggregate, record)
            end

        telemetry_stop(:load, start, metadata(aggregate), measurements(aggregate))
        {:noreply, listen(aggregate), Timer.seconds(30)}
    end

    @impl true
    def handle_call({:state, opts}, from, %Aggregate{}=aggregate) do
        %Aggregate{
            state: state,
            timeout: timeout,
            awaiting: waiting, 
        } = aggregate

        red = Keyword.get(opts, :version, aggregate.version)

        waiter = %Waiter{
            ref: nil, 
            vsn: red, 
            from: from, 
            timeout: Keyword.get(opts, :timeout, aggregate.wait)
        }

        meta = aggregate |> metadata() |> Map.put(:caller, metadata(waiter)) 

        start = telemetry_start(:state, meta, measurements(aggregate))

        if aggregate.version >= waiter.vsn do

            telemetry_stop(:state, start, meta, measurements(aggregate))
            {:reply, {:ok, state}, aggregate, timeout} 
        else
            ref = 
                from
                |> elem(0)
                |> Process.monitor()

            aggregate = %Aggregate{aggregate | 
                awaiting: waiting ++ List.wrap(%Waiter{waiter | ref: ref, start: start})
            }

            {:noreply, aggregate, waiter.timeout}
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
    def handle_info(%Event{number: number}, %Aggregate{ack: ack}=aggregate) 
    when number <= ack do
        {:noreply, aggregate, aggregate.timeout}
    end

    @impl true
    def handle_info(%Event{}=event, %Aggregate{}=aggregate) do
        start = telemetry_start(:reduce, metadata(aggregate), measurements(aggregate))
        case apply_event(aggregate, event) do
            {:ok, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                telemetry_stop(:reduce, start, metadata(aggregate), measurements(aggregate))
                {:noreply, aggregate, aggregate.timeout} 

            {:hibernate, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                telemetry_stop(:reduce, start, metadata(aggregate), measurements(aggregate))
                {:noreply, aggregate, :hibernate} 

            {:stop, reason, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                telemetry_stop(:reduce, start, metadata(aggregate), measurements(aggregate))
                {:stop, reason, aggregate} 

            error ->
                telemetry_stop(:reduce, start, metadata(aggregate), measurements(aggregate))
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
            |> Enum.filter(fn %Waiter{ref: wref} -> 
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
            state: state, 
            version: vsn, 
            awaiting: waiters
        } = aggregate

        awaiting =
            Enum.filter(waiters, fn %Waiter{}=waiter -> 
                %Waiter{from: from, ref: ref, start: start, vsn: wvsn} = waiter
                if vsn >= wvsn do
                    meta = 
                        aggregate
                        |> metadata() 
                        |> Map.put(:caller, metadata(waiter))

                    telemetry_stop(:state, start, meta, %{})
                    Process.demonitor(ref)
                    GenServer.reply(from, {:ok, state})
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

    def await(aggregate, stage, timeout \\ 5000) do
        opts = [version: stage, timeout: timeout] 
        state(aggregate, opts)
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
            %Event{position: position} when position == (version + 1) ->
                [
                    app: aggregate.app,
                    stream: aggregate.stream,
                    version: aggregate.version,
                    reducing: event.topic,
                    position: event.position,
                ]
                |> Signal.Logger.info(label: :aggregate)

                case Reducer.apply(state, Event.data(event)) do
                    %{__struct__: type}=state when is_atom(type) and type == stream_type ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position,
                            }
                        {:ok, aggregate}

                    {:ok, state}  ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position,
                            }
                        {:ok, aggregate}

                    {:ok, state, timeout} when is_number(timeout)  ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                timeout: timeout,
                                version: position,
                            }
                        {:ok, aggregate}


                    {:snapshot, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }
                            |> snapshot()

                        {:ok, aggregate}

                    {:snapshot, state, :sleep} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }
                            |> snapshot()

                        {:hibernate, aggregate}

                    {:snapshot, state, timeout} when is_number(timeout) ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                timeout: timeout,
                                version: position
                            }
                            |> snapshot()

                        {:ok, aggregate}

                    {:stop, reason, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }
                            |> snapshot()

                        {:stop, reason, aggregate}

                    {:sleep, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }

                        {:hibernate, aggregate}

                    {:sleep, state, timeout} when is_number(timeout) ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                timeout: timeout,
                                version: position
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

    defp acknowledge(%Aggregate{}=aggregate, %Event{number: number}) do
        %Aggregate{
            app: app, 
            consumer: consumer,
        } = aggregate

        app
        |> Signal.Event.Broker.acknowledge(consumer, number)

        %{aggregate| ack: number, consumer: %{ consumer| ack: number} }
    end

    defp snapshot(%Aggregate{}=aggregate) do
        %Aggregate{
            app: app, 
            state: state,
            stream: stream,
            version: version
        } = aggregate

        start = telemetry_start(:snapshot, metadata(aggregate), measurements(aggregate))
        {stream_id, _type} = stream

        {:ok, payload} = Codec.encode(state)

        [
            app: aggregate.app,
            stream: stream,
            status: :snapshoting,
            version: version,
        ]
        |> Signal.Logger.info(label: :aggregate)

        data = %{"state" => payload, "ack" => aggregate.ack}

        snapshot = 
            [id: stream_id, version: version, data: data]
            |> Snapshot.new()

        Signal.Store.Adapter.record_snapshot(app, snapshot)

        telemetry_stop(:snapshot, start, metadata(aggregate), measurements(aggregate))
        aggregate
    end

    def load_state(%Aggregate{state: state}=aggr, %Snapshot{}=snapshot) do
        %Snapshot{version: version, data: %{"state" => data}}=snapshot
        {:ok, state} = Codec.load(state, data)
        %Aggregate{aggr | 
            state: state, 
            version: version, 
        }
    end

    defp listen(%Aggregate{app: app, stream: stream, version: vsn}=aggr) do
        {stream_id, _stream_type} = stream

        streams = List.wrap(stream_id)

        start = 
            if vsn === 0 do
                0
            else
                %Signal.Event{number: start} = 
                    Signal.Store.Adapter.get_stream_event(app, stream_id, vsn)
                start
            end

        opts = [start: start, track: false, streams: streams]

        consumer = Signal.Event.Broker.subscribe(app, stream_id, opts)

        %Aggregate{aggr| consumer: consumer}
    end

    @impl true
    def terminate(:normal, %Aggregate{}=aggregate) do
        [
            app: aggregate.app,
            stream: aggregate.stream,
            status: :terminated,
            reason: :normal,
            version: aggregate.version,
        ]
        |> Signal.Logger.info(label: :aggregate)
    end

    @impl true
    def terminate(reason, %Aggregate{}=aggregate) do
        [
            id: aggregate.id,
            app: aggregate.app,
            stream: aggregate.stream,
            status: :terminated,
            reason: reason,
            version: aggregate.version,
        ]
        |> Signal.Logger.error(label: :aggregate)
    end

    def metadata(%Waiter{}=waiter) do
        %{
            caller: waiter.from,
            version: waiter.vsn,
            timeout: waiter.timeout,
        }
    end

    def metadata(%Aggregate{}=aggregate) do
        %{
            id: aggregate.id,
            app: aggregate.app,
            stream: aggregate.stream,
            module: aggregate.module,
        }
    end

    def measurements(%Aggregate{}=aggregate) do
        %{
            ack: aggregate.ack,
            version: aggregate.version,
            awaiting: length(aggregate.awaiting),
        }
    end

end
