defmodule Signal.Events.Producer do
    use GenServer, restart: :transient

    alias Signal.Multi
    alias Signal.Timer
    alias Signal.Result
    alias Signal.Events
    alias Signal.Transaction
    alias Signal.Events.Event
    alias Signal.Events.Stage
    alias Signal.Stream.History
    alias Signal.Command.Action
    alias Signal.Command.Handler
    alias Signal.Events.Producer

    defstruct [:app, :stream, position: 0]

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
    def handle_call(:cursor, _from, %Producer{ position: cursor}=state) do
        {:reply, cursor, state}
    end

    @impl true
    def handle_call({:stage, action, events}, from, %Producer{}=state)
    when is_list(events) do

        %Producer{app: app} = state

        channel =
            app
            |> Signal.Application.supervisor(Task)
            |> Task.Supervisor.async_nolink(fn ->
                receive do
                    {:ok, version} ->
                        {:ok, version}

                    {:rollback, reason} ->
                        {:rollback, reason}

                    error ->
                        error
                end
            end)

        stage = stage_events(state, action, events, channel.pid)

        %Stage{version: version} = stage

        GenServer.reply(from, stage)

        # Halt until the task is resolved
        case Task.yield(channel, :infinity) do
            {:ok, {:ok, ^version}} ->
                {:noreply, %Producer{state | position: version}, Timer.seconds(5)}

            {:ok, {:rollback, _}} ->
                {:noreply, calibrate(state), Timer.seconds(5)}

             _ ->
                {:noreply, calibrate(state), Timer.seconds(5)}
        end
    end

    @impl true
    def handle_call({:process, %Action{}=action}, _from, %Producer{}=producer) do

        %{stream: stream, app: app, position: position} = producer

        %Action{
            result: result,
            command: command, 
            snapshots: snapshots, 
        } = action

        {app_module, tenant} =  app

        aggregate = aggregate_state(producer, Signal.Sync.sync(command, result))

        event_streams =
            command
            |> handle_command(result, aggregate)
            |> group_events_by_stream()

        if is_map(event_streams) do
            case stage_event_streams(producer, action, event_streams) do
                {:ok, staged} ->
                    transaction = Transaction.new(staged, snapshots: snapshots)
                    case app_module.publish(transaction, [tenant: tenant]) do
                        :ok ->
                            confirm_staged(staged)

                            {stream_id, _} = stream

                            position = Enum.find_value(staged, position, fn
                                %{stream: ^stream_id, version: version} ->
                                    version
                                _ -> false
                            end)

                            histories = Enum.map(staged, fn staged_stream ->
                                struct(History, Map.from_struct(staged_stream))
                            end)

                            state = %Producer{producer| position: position}

                            {:reply, {:ok, histories}, state}

                        error ->
                            rollback_staged(staged, error)
                            {:reply, error, producer}
                    end

                {:error, reason} ->
                    {:reply, {:error, reason}, producer}
            end
        else
            {:reply, event_streams, producer, Timer.seconds(5)}
        end
    end

    def stage_event_streams(%Producer{}=producer, action, stream_events)
    when is_map(stream_events) do
        %Producer{app: app, stream: stream} = producer

        task_supervisor = Signal.Application.supervisor(app, Task)

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
                        |> Signal.Events.Supervisor.prepare_producer(stream)
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
                {:ok, stream_stages}

            {:error, reason} ->
                rollback_staged(stream_stages, reason)
                {:error, reason}
        end
    end

    def stage_events(producer, action, events) do
        GenServer.call(producer, {:stage, action, events})
    end

    def stage_events(%Producer{position: index, stream: stream}, action, events, stage)
    when is_list(events) and is_integer(index) and is_pid(stage) do
        {stream_id, _} = stream
        {events, version} =
            Enum.map_reduce(events, index, fn event, index ->
                opts = [
                    causation_id: action.causation_id,
                    correlation_id: action.correlation_id,
                ]
                event = Event.new(event, opts)
                {event, index + 1}
            end)
        %Stage{events: events, version: version, stream: stream_id, stage: stage}
    end

    def process(%Action{stream: stream, app: app}=action) do
        Events.Supervisor.prepare_producer(app, stream)
        |> GenServer.call({:process, action}, :infinity)
    end

    defp calibrate(%Producer{app: app, stream: {stream_id, _}}=prod) do
        {application, _tenant} = app
        case application.stream_position(stream_id) do
            nil ->
                %Producer{prod | position: 0}

            position ->
                %Producer{prod | position: position}
        end
    end

    defp aggregate_state(%Producer{}=producer, sync) do
        %Producer{ app: app, stream: stream, position: position} = producer
        state_opts =
            if sync do
                [version: position, timeout: :infinity]
            else
                []
            end
        Signal.Aggregates.Supervisor.prepare_aggregate(app, stream)
        |> Signal.Aggregates.Aggregate.state(state_opts)
    end

    defp handle_command(command, result, aggregate) when is_struct(command) do
        try do
            case Handler.handle(command, result, aggregate) do
                nil ->
                    []

                %Multi{events: events} ->
                    events

                {:ok, event} when is_struct(event) ->
                    [event]

                {:ok, event} when is_list(event) ->
                    event

                event when is_struct(event) ->
                    [event]

                event when is_list(event) ->
                    event

                {:error, reason} ->
                    Result.error(reason)
            end
        rescue
            raised ->
                {:error, :raised, {raised, __STACKTRACE__}}
        catch
            thrown ->
                {:error, :threw, {thrown, __STACKTRACE__}}
        end
    end

    # Pass through function
    defp group_events_by_stream(events) when is_list(events) do
        try do
            Enum.group_by(events, &event_stream!/1)
        rescue
            raised ->
                {:error, :raised, {raised, __STACKTRACE__}}
        catch
            thrown ->
                {:error, :threw, {thrown, __STACKTRACE__}}
        end
    end

    defp group_events_by_stream(unknown) do
        unknown
    end

    defp event_stream!(event) do
        stream = Signal.Stream.stream(event)
        case stream do
            {id, module} when is_atom(module) and is_binary(id) ->
                constructable!(stream)

            stream ->
                raise(Signal.Exception.InvalidStreamError, [stream: stream])
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

    defp confirm_staged(staged) do
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

    @impl true
    def terminate(reason, state) do
        Map.from_struct(state)
        |> Map.to_list()
        |> Enum.concat([shutdown: reason])
        |> Signal.Logger.info(label: :producer)
        :shutdown
    end

end
