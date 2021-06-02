defmodule Signal.Result do

    defstruct [:assigns, :result, aggregates: []]

    def ok(arg) do
        {:ok, arg}
    end

    def error(reason) do
        {:error, reason}
    end

    def ok?(returned) when is_tuple(returned), do: elem(returned, 0) == :ok
    def ok?(_), do: false

    def error?(returned) when is_tuple(returned), do: elem(returned, 0) == :error
    def error?(_), do: false

end
