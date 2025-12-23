defmodule Signal.Command.Dispatcher do
    use Signal.Telemetry

    alias Signal.Result
    alias Signal.Stream.Event
    alias Signal.Stream.History
    alias Signal.Command.Action
    alias Signal.Stream.Producer
    alias Signal.Execution.Queue
    alias Signal.Task, as: SigTask

    def dispatch(%SigTask{}=task) do
        tdata = metadata(task)
        start = telemetry_start(:dispatch, tdata, %{})
        result =
            case execute(task) do
                {:ok, result} ->
                    process(%SigTask{task | result: result})
                    |> finalize(task)

                error ->
                    error
            end
        telemetry_stop(:dispatch, start, tdata, %{})
        result
    end

    def process(%SigTask{}=task) do
        tdata = metadata(task)
        start = telemetry_start(:process, tdata, %{})
        action = Action.from(task)
        result = 
            case Producer.process(action) do
                {:ok, result} ->
                    {:ok, result}

                {:error, reason} ->
                    {:error, reason}

                crash when is_tuple(crash) ->
                    handle_crash(crash)
            end
        telemetry_stop(:process, start, tdata, %{})
        result
    end

    def execute(%SigTask{} =task) do
        %SigTask{app: app, command: command, assigns: assigns} = task
        tdata = metadata(task)
        start = telemetry_start(:execute, tdata, %{})
        result = 
            case Queue.handle(app, command, assigns, []) do
                {:ok, result} ->
                    {:ok, result}

                {:error, reason} ->
                    {:error, reason}

                crash when is_tuple(crash) ->
                    handle_crash(crash)

                result ->
                    {:ok, result}
            end
        telemetry_stop(:execute, start, tdata, %{})
        result
    end

    defp finalize({:ok, histories}, %SigTask{}=sig_task) do
        tdata = metadata(sig_task)
        start = telemetry_start(:finalize, tdata, %{})

        %SigTask{app: app, result: result, assigns: assigns, await: await} = sig_task

        events = Enum.reduce(histories, [], fn %History{events: events}, acc -> 
            acc ++ Enum.map(events, &Event.payload(&1))
        end)

        opts = [result: result, assigns: assigns, events: events]

        result = 
            if await do
                states = 
                    histories
                    |> Enum.map(fn %History{stream: stream, version: version} -> 
                        state_opts = [version: version, timeout: :infinity]
                        Task
                        |> Signal.Application.supervisor(app)
                        |> Task.Supervisor.async_nolink(fn -> 
                            {:ok, state} =
                                app
                                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
                                |> Signal.Aggregates.Aggregate.state(state_opts)
                            state
                        end, [shutdown: :brutal_kill])
                    end)
                    |> Task.yield_many(timeout(await))
                    |> Enum.map(fn {task, res} -> 
                        case res do
                            {:ok, agg} ->
                                agg
                            _ ->
                                Task.shutdown(task, :brutal_kill)
                                {:error, :task_timout}
                        end
                    end)

                struct(Result, opts ++ [states: states])
            else
                struct(Result, opts)
            end
            |> Result.ok()
        telemetry_stop(:finalize, start, tdata, %{})
        result
    end

    defp finalize({:error, reason}, %SigTask{}) do
        {:error, reason}
    end

    def timeout(true) do
        5000
    end

    def timeout(duration) do
        duration
    end

    def handle_crash({:error, :raised, {raised, stacktrace}}) do
        reraise(raised, stacktrace)
    end

    def handle_crash({:error, :threw, {thrown, _stacktrace}}) do
        throw(thrown)
    end

    defp metadata(%SigTask{}=task) do
        %{
            app: task.app,
            queue: task.queue,
            await: task.await,
            assigns: task.assigns,
            timestamp: task.timestamp,
            command: task.command_type,
            consistent: task.consistent,
            causation_id: task.causation_id,
            correlation_id: task.correlation_id
        }
    end

end
