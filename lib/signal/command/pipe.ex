defmodule Signal.Command.Pipe do

    alias Signal.Execution.Task

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

