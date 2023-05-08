defmodule Signal.Event do


    defmacro __using__(opts) do
        quote do
            use Blueprint.Schema
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @stream_opts Keyword.get(unquote(opts), :stream)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do

            with {:ok, topic} <- Module.get_attribute(__MODULE__, :topic, __MODULE__) do
                defimpl Signal.Name, for: __MODULE__ do
                    @topic (if is_binary(topic) do 
                        topic
                    else 
                        Signal.Helper.module_to_string(topic) 
                    end)

                    def name(_event) do 
                        @topic
                    end
                end
            end

            if Module.defines?(__MODULE__, {:apply, 2}, :def) do
                with module <- @module do
                    defimpl Signal.Stream.Reducer do
                        @pmodule module
                        def apply(event, agg) do 
                            Kernel.apply(@pmodule, :apply, [event, agg])
                        end
                    end
                end
            end

            with stream_opts when is_tuple(stream_opts) 
                 <- Module.get_attribute(__MODULE__, :stream_opts) do
                require Signal.Impl.Stream
                Signal.Impl.Stream.impl(stream_opts)
            end

        end
    end

    use Signal.Type
    require Logger

    alias Signal.Codec
    alias Signal.Event
    alias Signal.Helper

    defmodule Metadata do

        use Signal.Type

        schema enforce: true do
            field :uuid,            String.t()
            field :topic,           String.t()
            field :number,          integer()
            field :position,        integer()
            field :stream_id,       String.t()
            field :causation_id,    String.t()
            field :correlation_id,  String.t()
            field :timestamp,       term()
        end

    end

    schema enforce: true do
        field :uuid,            String.t()
        field :topic,           String.t()
        field :number,          integer()
        field :position,        integer()
        field :data,            map()
        field :stream_id,       String.t()
        field :causation_id,    String.t()
        field :correlation_id,  String.t()
        field :timestamp,       term()
    end

    def data(%Event{data: payload, topic: topic}) do
        module = Helper.string_to_module(topic)

        try do
            {:ok, data} =
                module
                |> struct([])
                |> Codec.load(payload)
            data
        rescue
            UndefinedFunctionError ->
                msg = """
                Could not create event instance: #{topic}
                fallback to map instance
                """
                Logger.error(msg)
                Map.put(payload, :__struct__, module)

            exception ->
                reraise(exception, __STACKTRACE__)
        end
    end

    def metadata(%Event{}=event) do
        Metadata.from(event)
    end
end
