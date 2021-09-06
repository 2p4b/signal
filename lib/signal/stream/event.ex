defmodule Signal.Stream.Event do
    use Timex
    use Signal.Type

    alias Signal.Codec
    alias Signal.Helper
    alias Signal.Stream.Event

    defmodule Metadata do

        use Signal.Type

        schema enforce: true do
            field :uuid,            String.t()
            field :type,            String.t()
            field :topic,           String.t()
            field :stream,          String.t()
            field :position,        integer()
            field :number,          integer()
            field :causation_id,    String.t()
            field :correlation_id,  String.t()
            field :timestamp,       term()
        end

    end

    schema enforce: true do
        field :uuid,            String.t()
        field :topic,           String.t()
        field :stream,          String.t()
        field :data,            map()
        field :type,            String.t()
        field :position,        integer()
        field :number,          integer(),  default: nil
        field :causation_id,    String.t()
        field :correlation_id,  String.t()
        field :timestamp,       term()
    end

    def encode(%{__struct__: type}=payload) when is_struct(payload) do
        {Helper.module_to_string(type), Codec.encode(payload)}
    end

    def payload(%Event{data: data, type: type}) do
        module = Helper.string_to_module(type)
        Codec.load(struct(module, []), data)
    end

    def metadata(%Event{}=event) do
        Metadata.from(event)
    end

end

