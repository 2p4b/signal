defmodule Signal.Task do
    alias Signal.Task

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
        snapshots: [],
        handles: [],
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

    def assign(%Task{assigns: assigns}=task, key, value) 
    when is_binary(key) or is_atom(key) do
        %Task{ task | assigns: Map.put(assigns, key, value) }
    end

    def assign(%Task{assigns: assigns}=task, params) when is_list(params) do
        %Task{ task | assigns: Enum.reduce(params, assigns, fn({key, value}, acc) ->
            Map.put(acc, key, value)
        end) }
    end

end
