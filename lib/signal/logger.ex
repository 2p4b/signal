defmodule Signal.Logger do
    require Logger

    def info(fields, opts \\ [])
    def info(fields, opts) when is_map(fields) do
        fields
        |> Map.to_list()
        |> info(opts)
    end

    def info(fields, opts) when is_list(fields) do
        fields
        |> Enum.reduce([], fn {key, value}, acc -> 
            fname = String.Chars.to_string(key) 
            key_value =
                [fname, ": ", inspect(value)]
                |> Enum.join()
                |> List.wrap()
            Enum.concat(acc, key_value)
        end)
        |> Enum.concat([""])
        |> Enum.join("\n")
        |> info(opts)
    end

    def info(text, opts) when is_binary(text) do
        case Keyword.fetch(opts, :label) do
            {:ok, label} ->
                name = 
                    label
                    |> String.Chars.to_string() 
                    |> String.upcase()

                ["[", name, "]", "\n", text]
                |> Enum.join()

            _ -> text
        end
        |> Logger.info()
    end
end
