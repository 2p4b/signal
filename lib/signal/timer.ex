defmodule Signal.Timer do

    def hrs(value), do: hours(value)
    def min(value), do: minutes(value)
    def sec(value), do: seconds(value)

    def seconds(value) when is_number(value) do
        value * 1000
    end

    def minutes(value) when is_number(value) do
        value * seconds(60)
    end

    def hours(value) when is_number(value) do
        value * minutes(60)
    end

    def days(value) when is_number(value) do
        value * hours(24)
    end

    def apply(module, method, args \\ nil) 
    def apply(func, args, _args) 
    when is_function(func) and is_list(args) do
        :timer.tc(func, args)
    end

    def apply(module, method, args) 
    when is_atom(module) and is_atom(method) and is_list(args) do
        :timer.tc(module, method, args)
    end

end
