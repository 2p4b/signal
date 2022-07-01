defmodule Signal.Stream.Identifier do
    alias Signal.Helper

    def id({id, module}) when is_atom(module) and is_binary(id) do
        type =
            module
            |> Helper.module_to_string() 
            |> String.downcase()
        id(id, type)
    end

    def id({id, type}) when is_binary(type) and is_binary(id) do
        id(id, type)
    end

    def id(id, type) when is_binary(type) and is_binary(id) do
        type <> "://" <> id
    end

end

