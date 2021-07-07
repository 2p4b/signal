defmodule Signal.Event do


    defmodule Multi do

        alias Signal.Event.Multi

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
                        events + event

                    event when is_struct(event) ->
                        events + List.wrap(event)

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

    defmacro __using__(opts) do
        quote do
            use Signal.Type
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @topic Keyword.fetch(unquote(opts), :topic)
            @stream Keyword.get(unquote(opts), :stream)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do

            with {:ok, topic} <- Module.get_attribute(__MODULE__, :topic) do
                defimpl Signal.Topic, for: __MODULE__ do
                    @topic (if is_binary(topic) do 
                        topic
                    else 
                        Signal.Helper.module_to_string(topic) 
                    end)

                    def topic(_event) do 
                        @topic
                    end
                end
            end

            with {module, key} <- Module.get_attribute(__MODULE__, :stream) do
                defimpl Signal.Stream, for: __MODULE__ do
                    @field key
                    @stream_module module
                    def stream(command, _res) do 
                        {@stream_module, Map.get(command, @field)}
                    end
                end
            end

        end
    end

end
