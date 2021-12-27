defmodule Signal.Events.Event do
    use Signal.Type

    alias Signal.Codec
    alias Signal.Helper
    alias Signal.Events.Event

    schema enforce: true do
        field :topic,           String.t()
        field :data,            map()
        field :type,            atom()
        field :timestamp,       term()
        field :causation_id,    String.t(),     default: nil
        field :correlation_id,  String.t(),     default: nil
    end

    def new(event, opts) when is_struct(event) do
        {type, data} = encode(event)
        params = [
            data: data,
            type: type,
            topic: Signal.Topic.topic(event), 
            timestamp: DateTime.utc_now(),
        ]
        struct(__MODULE__, params ++ opts)
    end

    defp encode(%{__struct__: type}=payload) when is_struct(payload) do
        {Helper.module_to_string(type), Codec.encode(payload)}
    end

    def payload(%Event{data: data, type: type}) do
        module = Helper.string_to_module(type)
        Codec.load(struct(module, []), data)
    end

end
