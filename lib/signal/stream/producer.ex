defmodule Signal.Stream.Producer do
    use GenServer, restart: :transient
    use Signal.Telemetry

    alias Signal.Multi
    alias Signal.Timer
    alias Signal.Result
    alias Signal.Transaction
    alias Signal.Stream.Event
    alias Signal.Stream.Stage
    alias Signal.Store.Writer
    alias Signal.Stream.History
    alias Signal.Command.Action
    alias Signal.Command.Handler
    alias Signal.Stream.Producer

    defstruct [:app, :stream, index: 0]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name, __MODULE__)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        Process.send(self(), :init, [])
        {:ok, struct(__MODULE__, opts)}
    end

    @impl true
    def handle_info(:init, %Producer{}=state) do
        {:noreply, calibrate(state)}
    end

    @impl true
    def handle_info(:timeout, %Producer{}=state) do
        {:stop, :normal, state}
    end

    @impl true
    def handle_call(:cursor, _from, %Producer{index: index}=state) do
        {:reply, index, state}
    end

    @impl true
    def handle_call({:stage, action, events}, from, %Producer{}=producer)
    when is_list(events) do

        %Producer{app: app} = producer

        meta = metadata(producer, action)
        measure = measurements(producer)
        start = telemetry_start(:state, meta, measure)

        channel =
            Task
            |> Signal.Application.supervisor(app)
            |> Task.Supervisor.async_nolink(fn ->
                receive do
                    {:ok, index} ->
                        {:ok, index}

                    {:rollback, reason} ->
                        {:rollback, reason}

                    error ->
                        error
                end
            end)

        stage = stage_events(producer, action, events, channel.pid)

        %Stage{version: version} = stage

        GenServer.reply(from, stage)

        # Halt until the task is resolved
        case Task.yield(channel, :infinity) do
            {:ok, {:ok, ^version}} ->
                producer = %Producer{producer | index: version}
                meta = metadata(producer, action) 
                telemetry_stop(:state, start, meta, measure)
                {:noreply, producer, Timer.seconds(5)}

            {:ok, {:rollback, _}} ->
                producer = calibrate(producer)
                meta = metadata(producer, action) 
                telemetry_stop(:state, start, meta, measure)
                {:noreply, producer, Timer.seconds(5)}

             _ ->
                producer = calibrate(producer)
                meta = metadata(producer, action) 
                telemetry_stop(:state, start, meta, measure)
                {:noreply, producer, Timer.seconds(5)}
        end
    end

    @impl true
    def handle_call({:process, %Action{}=action}, _from, %Producer{}=producer) do
        %Action{
            result: result,
            command: command, 
        } = action

        meta = metadata(producer, action)

        start = telemetry_start(:process, meta, measurements(producer))

        result = 
            command
            |> Signal.Sync.sync(result)
            |> aggregate_state(action, producer)
            |> handle_command(action, producer)
            |> group_events_by_stream()
            |> stage_event_streams(action, producer)
            |> commit_staged_transaction(action, producer)

        case result do
            {:ok, %{index: index, history: history}} ->
                producer = %Producer{producer | index: index}
                telemetry_stop(:process, start, meta, measurements(producer))
                {:reply, {:ok, history}, producer, Timer.seconds(5)}

            {:error, reason} ->
                telemetry_stop(:process, start, meta, measurements(producer))
                {:reply, {:error, reason}, producer, Timer.seconds(5)}
        end
    end

    def commit_staged_transaction({:ok, staged}, %Action{}=action, %Producer{}=producer) do
        %Producer{app: app, stream: stream} = producer
        meta = metadata(producer, action)
        measure = measurements(producer)
        start = telemetry_start(:commit, meta, measure)
        transaction = Transaction.new(staged)
        case Writer.commit(app, transaction, []) do
            :ok ->
                confirm_staged(staged)

                index = Enum.find_value(staged, producer.index, fn
                    %{stream: ^stream, version: position} ->
                        position
                    _ -> false
                end)

                history = Enum.map(staged, fn staged_stream ->
                    struct(History, Map.from_struct(staged_stream))
                end)

                telemetry_stop(:commit, start, meta, measure)
                {:ok, %{index: index, history: history}}

            error ->
                rollback_staged(staged, error)
                meta = Map.put(meta, :error, error)
                telemetry_stop(:commit, start, meta, measure)
                {:error, error}
        end
    end
    def commit_staged_transaction(error, %Action{}, %Producer{}) do
        error
    end

    def stage_event_streams(stream_events, action, %Producer{}=producer)
    when is_map(stream_events) do
        %Producer{app: app, stream: stream} = producer

        meta = metadata(producer, action)
        measure = measurements(producer)
        start = telemetry_start(:stage_stream, meta, measure)

        task_supervisor = Signal.Application.supervisor(Task, app)

        stream_stages =
            stream_events
            |> Enum.map(fn
                {^stream, events} ->
                    Task.Supervisor.async_nolink(task_supervisor, fn ->
                        stage_events(producer, action, events, self())
                    end)


                {stream, events} ->
                    # Process event steams in parallel
                    Task.Supervisor.async_nolink(task_supervisor, fn ->
                        app
                        |> Signal.Stream.Supervisor.prepare_producer(stream)
                        |> stage_events(action, events)
                    end)

            end)
            |> Task.yield_many(:infinity)
            |> Enum.map(fn {_task, res} ->
                case res do
                    {:ok, resp} ->
                        resp

                    {:exit, reason} ->
                        {:error, reason}
                end
            end)

        case Enum.find(stream_stages, :ok, &Result.error?/1) do
            :ok ->
                telemetry_stop(:stage_stream, start, meta, measure)
                {:ok, stream_stages}

            {:error, reason} ->
                rollback_staged(stream_stages, reason)
                meta = Map.put(meta, :error, reason)
                telemetry_stop(:stage_stream, start, meta, measure)
                {:error, reason}
        end
    end

    def stage_event_streams(error, _action, %Producer{}) do
        error
    end

    def stage_events(producer, action, events) do
        GenServer.call(producer, {:stage, action, events})
    end

    def stage_events(%Producer{index: index, stream: stream}, action, events, stage)
    when is_list(events) and is_integer(index) and is_pid(stage) do
        {stream_id, _}  = stream
        {events, version} =
            Enum.map_reduce(events, index, fn event, position ->
                position =  position + 1
                params = [
                    position: position,
                    stream_id: stream_id,
                    causation_id: action.causation_id,
                    correlation_id: action.correlation_id,
                ]
                event = Event.new(event, params)
                {event, position}
            end)
        %Stage{events: events, version: version, stream: stream, stage: stage}
    end

    def process(%Action{stream: stream, app: app}=action) do
        Signal.Stream.Supervisor.prepare_producer(app, stream)
        |> GenServer.call({:process, action}, :infinity)
    end

    def calibrate(%Producer{app: app, stream: {stream_id, _}}=prod) do
        case Signal.Store.Adapter.stream_position(app, stream_id) do
            nil ->
                %Producer{prod | index: 0}

             index->
                %Producer{prod | index: index}
        end
    end

    def aggregate_state(sync, %Action{}=action, %Producer{}=producer) do
        meta = metadata(producer, action)
        measure = measurements(producer)
        start = telemetry_start(:state, meta, measure)
        %Producer{app: app, stream: stream, index: index} = producer
        state_opts =
            case sync do
                false ->
                    []

                true ->
                    [version: index, timeout: Timer.seconds(5)]

                :infinity ->
                    [version: index, timeout: :infinity]

                value when is_integer(value) and value > 0 ->
                    [version: index, timeout: value]

                _  ->
                    raise(ArgumentError, "invalid stream sync: #{sync}")
            end
        state =
            Signal.Aggregates.Supervisor.prepare_aggregate(app, stream)
            |> Signal.Aggregates.Aggregate.state(state_opts)
        telemetry_stop(:state, start, meta, measure)
        state
    end

    def handle_command({:ok, aggregate}, %Action{command: command, result: result}=action, %Producer{}=producer) 
    when is_struct(command) do
        meta = metadata(producer, action)
        measure = measurements(producer)
        start = telemetry_start(:handle, meta, measure)
        try do
            case Handler.handle(command, result, aggregate) do
                nil ->
                    telemetry_stop(:handle, start, meta, measure)
                    []

                %Multi{events: events} ->
                    telemetry_stop(:handle, start, meta, measure)
                    events

                {:ok, event} when is_struct(event) ->
                    telemetry_stop(:handle, start, meta, measure)
                    List.wrap(event)

                {:ok, event} when is_list(event) ->
                    telemetry_stop(:handle, start, meta, measure)
                    event

                event when is_struct(event) ->
                    telemetry_stop(:handle, start, meta, measure)
                    List.wrap(event)

                event when is_list(event) ->
                    telemetry_stop(:handle, start, meta, measure)
                    event

                {:error, reason} ->
                    meta = Map.put(meta, :error, reason)
                    telemetry_stop(:handle, start, meta, measure)
                    Result.error(reason)
            end
        rescue
            raised ->
                error = {:raised, raised, __STACKTRACE__}
                meta = Map.put(meta, :error, error)
                telemetry_stop(:handle, start, meta, measure)
                {:error, error}

        catch
            thrown ->
                error = {:threw, thrown, __STACKTRACE__}
                meta = Map.put(meta, :error, error)
                telemetry_stop(:handle, start, meta, measure)
                {:error, error}
        end
    end

    def handle_command(error, %Action{}, %Producer{}) do
        error
    end

    # Pass through function
    def group_events_by_stream(events) when is_list(events) do
        try do
            Enum.group_by(events, &event_stream!/1)
        rescue
            raised ->
                {:error, {:raised, raised, __STACKTRACE__}}
        catch
            thrown ->
                {:error, {:threw, thrown, __STACKTRACE__}}
        end
    end

    def group_events_by_stream(unknown) do
        unknown
    end

    defp event_stream!(event) do
        stream = Signal.Stream.stream(event)
        case stream do
            {id, module} when is_atom(module) and is_binary(id) ->
                constructable!(stream)

            stream ->
                raise(Signal.Exception.InvalidStreamError, [stream: stream, signal: event])
        end
    end

    defp constructable!({_, module}=stream) do
        try do
            struct(module, [])
            stream
        rescue
            _error ->
                raise(Signal.Exception.InvalidStreamError, [stream: stream])
        end
    end

    def confirm_staged(staged) do
        Enum.each(staged, fn %Stage{version: version, stage: stage}->
            Process.send(stage, {:ok, version}, [])
        end)
    end

    defp rollback_staged(staged, error) when is_list(staged) do
        reason =
            case error do
                {:error, reason} ->
                    reason
                _ -> nil
            end
        Enum.each(staged, fn
            %Stage{stage: stage} ->
                Process.send(stage, {:rollback, reason}, [:nosuspend])
            _ ->
                nil
        end)
    end

    def metadata(%Producer{}=producer, %Action{}=action) do
        %{
            app: producer.app,
            stream: producer.stream,
            action: action.command.__struct__,
        }
    end

    def measurements(%Producer{}=producer) do
        %{
            index: producer.index,
        }
    end

    @impl true
    def terminate(reason, state) do
        state
        |> Map.from_struct()
        |> Map.to_list()
        |> Enum.concat([shutdown: reason])
        |> Signal.Logger.info(label: :producer)
        :shutdown
    end

end
