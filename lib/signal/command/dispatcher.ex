defmodule Signal.Command.Dispatcher do

    alias Signal.Result
    alias Signal.Events.Event
    alias Signal.Stream.History
    alias Signal.Command.Action
    alias Signal.Events.Record
    alias Signal.Events.Producer
    alias Signal.Execution.Queue
    alias Signal.Execution.Task, as: SigTask

    def dispatch(%SigTask{}=task) do
        case execute(task) do
            {:ok, result} ->
                process(%SigTask{task | result: result})
                |> finalize(task)

            error ->
                error
        end
    end

    def process(%SigTask{}=task) do
        case Producer.process(Action.from(task)) do
            {:ok, result} ->
                {:ok, result}

            error ->
                error
        end
    end

    def execute(%SigTask{app: app, command: command, assigns: assigns}) do
        case Queue.handle(app, command, assigns, []) do
            {:ok, result} ->
                {:ok, result}

            {:error, reason} ->
                {:error, reason}

            result ->
                {:ok, result}
        end
    end

    defp finalize({:ok, record}, %SigTask{}=sig_task) do

        %Record{index: index, histories: histories} = record

        %SigTask{app: app, result: result, assigns: assigns, await: await} = sig_task

        events = Enum.reduce(histories, [], fn %History{events: events}, acc -> 
            acc ++ Enum.map(events, &Event.payload(&1))
        end)

        opts = [result: result, assigns: assigns, events: events, index: index]

        if await do
            aggregates = 
                histories
                |> Enum.map(fn %History{stream: stream, version: version} -> 
                    app
                    |> Signal.Application.supervisor(Task)
                    |> Task.Supervisor.async_nolink(fn -> 
                        app
                        |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
                        |> Signal.Aggregates.Aggregate.await(version, :infinity)
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

            struct(Result, opts ++ [aggregates: aggregates])
        else
            struct(Result, opts)
        end
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

end
