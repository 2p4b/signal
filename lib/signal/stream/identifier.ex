defmodule Signal.Stream.Identifier do
    alias Signal.Helper

    def id({module, id}) when is_atom(module) and is_binary(id) do
        module
        |> Helper.module_to_string() 
        |> String.downcase()
        |> id(id)
    end

    def id({type, id}) when is_binary(type) and is_binary(id) do
        id(type, id)
    end

    def id(type, id) when is_binary(type) and is_binary(id) do
        type <> "://" <> id
    end

end

