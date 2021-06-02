defmodule Signal.Execution.Task do
    alias Signal.Execution.Task

    defstruct [
        :app,
        :queue,
        :result,
        :timeout,
        :command,
        :timestamp,
        :command_type,
        :causation_id,
        :correlation_id,
        states: [],
        indices: [],
        assigns: %{},
        await: false,
        consistent: false
    ]

    def new(command, opts \\ []) do
        Task
        |> struct(opts)
        |> apply_cause()
        |> apply_correlation()
        |> apply_assigments()
        |> apply_command(command)
    end

    defp apply_cause(%Task{causation_id: nil}=task) do
        %Task{task | causation_id: UUID.uuid4()}
    end

    defp apply_cause(%Task{}=task) do
        task
    end

    defp apply_correlation(%Task{correlation_id: nil}=task) do
        %Task{task | correlation_id: UUID.uuid4()}
    end

    defp apply_correlation(%Task{}=task) do
        task
    end

    defp apply_assigments(%Task{}=task) do
        %Task{task | assigns: %{}}
    end

    defp apply_command(%Task{}=task, command) do
        %Task{task | command: command}
    end

end

