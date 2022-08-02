defmodule Signal.Events.Event do
    use Signal.Type

    alias Signal.Codec
    alias Signal.Helper
    alias Signal.Events.Event

    schema enforce: true do
        field :topic,           String.t()
        field :payload,         map()
        field :timestamp,       term()
        field :causation_id,    String.t(),     default: nil
        field :correlation_id,  String.t(),     default: nil
    end

    def new(event, opts) when is_struct(event) do
        {:ok, data} = Codec.encode(event)
        params = [
            payload: data,
            topic: Signal.Topic.topic(event), 
            timestamp: DateTime.utc_now(),
        ]
        struct(__MODULE__, params ++ opts)
    end


    def payload(%Event{payload: payload, topic: type}) do
        module = Helper.string_to_module(type)
        {:ok, event} = Codec.load(struct(module, []), payload)
        event
    end

end
