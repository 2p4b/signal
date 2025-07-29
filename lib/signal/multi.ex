defmodule Signal.Multi do

    alias Signal.Multi

    defstruct [:command, :events]

    def new(command) when is_struct(command) do
        struct(Multi, [command: command, events: []]) 
    end

    def apply(%Multi{command: command, events: events}, fun, args \\ []) 
    when is_atom(fun) when is_list(args) do
        %{__struct__: module} =  command
        events =
            case Kernel.apply(module, fun, List.wrap(command) ++ args) do
                event when is_list(event) ->
                    events ++ event

                {:ok, event} when is_list(event) -> 
                    events ++ event

                %Multi{}=multi ->
                    events ++ Multi.emit(multi)

                event when is_struct(event) ->
                    events ++ List.wrap(event)

                {:ok, event} when is_stuct(event) -> 
                    events ++ List.wrap(event)

                nil ->
                    events

                event -> 
                    raise(Signal.Exception.InvalidEventError, [event: event])

            end
        %Multi{events: events, command: command}
    end

    def emit(%Multi{events: events}) do
        events
    end

end


