defmodule Signal.Stream.Event do
    use Signal.Type
    require Logger

    alias Signal.Codec
    alias Signal.Helper
    alias Signal.Stream.Event

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
        field :payload,         map()
        field :stream_id,       String.t()
        field :causation_id,    String.t()
        field :correlation_id,  String.t()
        field :timestamp,       term()
    end

    def payload(%Event{payload: payload, topic: topic}) do
        module = Helper.string_to_module(topic)

        try do
            {:ok, event_payload} =
                module
                |> struct([])
                |> Codec.load(payload)
            event_payload
        rescue
            UndefinedFunctionError ->
                msg = """
                Could not create event instance: #{topic}
                fallback to map instance
                """
                Logger.error(msg)
                %{__struct__: module}

            exception ->
                reraise(exception, __STACKTRACE__)
        end
    end

    def metadata(%Event{}=event) do
        Metadata.from(event)
    end

end

