defmodule Signal.Helper do

    def stream_tuple(module, id, opts \\ []) when is_atom(module) do
        case Keyword.fetch(opts, :prefix) do
            {:ok, prefix} ->
                  stream_id = 
                      [prefix, id] 
                      |> Enum.join() 
                      |> String.trim()
                  {stream_id, module}
            _ ->
                  {id, module}
        end
    end

    def module_to_string(module) when is_atom(module) do
        case Atom.to_string(module) do
            "Elixir."<> name -> name
            name -> name
        end
    end

    def string_to_module(string) when is_binary(string) do
        string
        |> String.split(".")
        |> Module.concat()
    end

end
