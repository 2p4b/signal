defmodule Signal.Helper do

    def module_to_string(module) when is_atom(module) do
        string =
            module
            |> Atom.to_string()

        case string do
            "Elixir."<>_rest->
                string
                |> String.split(".")
                |> (fn [_elixir | name] -> name end).()
                |> Enum.join(".")

            str ->
                str
        end
    end

    def string_to_module(string) when is_binary(string) do
        string
        |> String.split(".")
        |> Module.concat()
    end

end
