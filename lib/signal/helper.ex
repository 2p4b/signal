defmodule Signal.Helper do

    def stream_id(id, opts \\ []) when is_binary(id) do
        case Keyword.fetch(opts, :prefix) do
            {:ok, prefix} ->
                  [prefix, id] 
                  |> Enum.join() 
                  |> String.trim()
            _ ->
                  id
        end
    end

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
        atom_to_string(module)
    end

    def atom_to_string(value) when is_atom(value) do
        case Atom.to_string(value) do
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
