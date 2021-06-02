defmodule Signal.Events.Event do
    use Timex
    use Signal.Type

    alias Signal.Stream
    alias Signal.Helper
    alias Signal.Events.Event
    alias Signal.Command.Action

    defmodule Meta do

        use Signal.Type

        schema enforce: true do
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
        field :payload,         term()
        field :reduction,       integer()
        field :number,          integer(),  default: nil
        field :causation_id,    String.t()
        field :correlation_id,  String.t()
        field :timestamp,       term()
    end

    def new(event, %Action{stream: source}=action, reduction) 
    when is_struct(event) and is_number(reduction) do
        params = [
            uuid: UUID.uuid4(), 
            topic: Event.topic(event), 
            stream: Stream.stream(event),
            source: source,
            payload: event,
            reduction: reduction,
            timestamp: Timex.now(),
            causation_id: action.causation_id,
            correlation_id: action.correlation_id,
        ]
        struct(__MODULE__, params)
    end

    def topic(%{__struct__: type}=event) when is_struct(event) do
        Helper.module_to_string(type)
    end
    
    def index(%Event{number: nil}=event, number) when is_integer(number) do
        %Event{event | number: number}
    end

    def meta(%Event{}=event) do
        Meta.from(event)
    end

end
