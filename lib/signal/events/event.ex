defmodule Signal.Events.Event do
    use Timex
    use Signal.Type

    alias Signal.Codec
    alias Signal.Stream
    alias Signal.Events.Event
    alias Signal.Command.Action

    defmodule Metadata do

        use Signal.Type

        schema enforce: true do
            field :type,            atom()
            field :uuid,            String.t()
            field :topic,           String.t()
            field :stream,          String.t()
            field :reduction,       integer()
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
        field :type,            atom()
        field :reduction,       integer()
        field :number,          integer(),  default: nil
        field :causation_id,    String.t()
        field :correlation_id,  String.t()
        field :timestamp,       term()
    end

    def new(event, %Action{stream: source}=action, reduction) 
    when is_struct(event) and is_number(reduction) do
        {type, data} = encode(event)
        params = [
            uuid: UUID.uuid4(), 
            topic: Signal.Topic.topic(event), 
            stream: Stream.stream(event),
            data: data,
            type: type,
            source: source,
            reduction: reduction,
            timestamp: Timex.now(),
            causation_id: action.causation_id,
            correlation_id: action.correlation_id,
        ]
        struct(__MODULE__, params)
    end

    defp encode(%{__struct__: type}=payload) when is_struct(payload) do
        {type, Codec.encode(payload)}
    end

    def payload(%Event{data: data, type: type}) do
        Codec.load(struct(type, []), data)
    end

    def index(%Event{number: nil}=event, number) when is_integer(number) do
        %Event{event | number: number}
    end

    def metadata(%Event{}=event) do
        Metadata.from(event)
    end

end
