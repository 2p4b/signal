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
        ack: 0, 
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
    def handle_call({:await, red}, from, %Aggregate{}=aggregate) do
        %Aggregate{
            version: vsn, 
            awaiting: waiting, 
            state: state
        } = aggregate

        if vsn >= red do
            {:reply, state, aggregate} 
        else
            ref = Process.monitor(elem(from, 0))
            {:noreply, %Aggregate{aggregate | awaiting: waiting ++ [{from, ref, red}]} }
        end
    end

    @impl true
    def handle_info(:init, %Aggregate{}=aggregate) do
        %Aggregate{
            app: {application, tenant}, 
            stream: {stream_id, _}
        } = aggregate

        aggregate = 
            case application.snapshot(stream_id, tenant: tenant) do
                nil -> 
                    aggregate

                snapshot ->
                    load(aggregate, snapshot)
            end
        {:noreply, listen(aggregate)}
    end

    @impl true
    def handle_info(%Event{number: number}, %Aggregate{ack: ack}=aggregate) 
    when number <= ack do
        {:noreply, aggregate}
    end

    @impl true
    def handle_info(%Event{}=event, %Aggregate{}=aggregate) do
        case apply_event(aggregate, event) do
            {:ok, %Aggregate{}=aggregate} ->
                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()
                {:noreply, aggregate} 

            {:sleep, %Aggregate{}=aggregate} ->

                aggregate =
                    aggregate
                    |> acknowledge(event)
                    |> reply_waiters()

                case aggregate.awaiting  do
                    [] ->
                        {:stop, :sleep, aggregate} 
                    _ ->
                        # NO Sleep if there are waiters
                        {:noreply, aggregate}
                end

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
        {:stop, :normal, aggregate}
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

    defp apply_event(%Aggregate{}=aggregate, %Event{number: number}=event) do
        %Aggregate{
            version: version, 
            state: state, 
        } = aggregate

        case event do
            %Event{position: position} when position == (version + 1) ->

                metadata = Event.metadata(event)
                
                event_payload = Event.payload(event)

                info = """
                applying: #{event.topic}
                position: #{event.position}
                number: #{event.number}
                """
                log(info, aggregate)

                case Reducer.apply(state, metadata, event_payload) do
                    {action, state} when action in [:ok, :sleep, :hibernate] ->
                         aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }
                        {action, aggregate}


                    {:snapshot, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }
                            |> snapshot()

                        {:ok, aggregate}

                    {:snapshot, state, action} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }
                            |> snapshot()

                        {action, aggregate}

                    {:stop, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }

                        {:stop, :normal, aggregate}

                    {:stop, reason, state} ->
                        aggregate = 
                            %Aggregate{aggregate | 
                                ack: number,
                                state: state,
                                version: position
                            }

                        {:stop, reason, aggregate}

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

        {application, tenant} = app
        application.acknowledge(handle, number, [tenant: tenant])

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

        stream_id
        |> Snapshot.new(payload, version: version)
        |> application.record()

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

    defp listen(%Aggregate{app: app, stream: {stream_id, _}, ack: ack}=aggr) do
        {application, _tenant} = app
        {:ok, subscription} = application.subscribe([
            start: ack,
            track: false, 
            stream: stream_id, 
        ])
        %Aggregate{aggr| subscription: subscription}
    end

    @impl true
    def terminate(reason, %Aggregate{}=aggregate) do
        "reason: #{inspect(reason)}"
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
