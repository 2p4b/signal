defmodule Signal.Result do

    alias Signal.Result

    defstruct [:index, :assigns, :result, states: [], events: []]

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

    def assigned(%Result{assigns: assigns}, value), do: Map.get(assigns, value)
    def assigns(%Result{assigns: assigns}), do: assigns
    def states(%Result{states: states}), do: states
    def result(%Result{result: result}), do: result
    def events(%Result{events: events}), do: events

end
