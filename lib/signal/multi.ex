defmodule Signal.Multi do

    alias Signal.Multi

    defstruct [:args, :events]

    def new(command, params, aggregate) when is_struct(command) do
        args = [command, params, aggregate]
        struct(Multi, [args: args, events: []]) 
    end

    def execute(%Multi{args: args, events: events}, fun) when is_atom(fun) do
        [%{__struct__: module}, _, _] =  args
        events =
            case Kernel.apply(module, fun, args) do
                event when is_list(event) ->
                    events ++ event

                event when is_struct(event) ->
                    events ++ List.wrap(event)

                nil ->
                    events

                event -> 
                    raise(Signal.Exception.InvalidEventError, [event: event])

            end
        %Multi{events: events, args: args}
    end

    def emit(%Multi{events: events}) do
        events
    end

end


