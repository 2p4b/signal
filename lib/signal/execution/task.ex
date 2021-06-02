defmodule Signal.Execution.Task do
    alias Signal.Execution.Task

    defstruct [
        :app,
        :uuid,
        :queue,
        :result,
        :timeout,
        :command,
        :timestamp,
        :command_type,
        :correlation_uuid,
        states: [],
        indices: [],
        assigns: %{},
        await: false,
        consistent: false,
        causation_type: :generic,
    ]

    def new(command, opts \\ []) do
        Task
        |> struct(opts)
        |> apply_uuid()
        |> apply_assigments()
        |> apply_command(command)
    end

    defp apply_uuid(%Task{}=task) do
        %Task{task | uuid: UUID.uuid4()}
    end

    defp apply_assigments(%Task{}=task) do
        %Task{task | assigns: %{}}
    end

    defp apply_command(%Task{}=task, command) do
        %Task{task | command: command}
    end

end

