defmodule Signal.Stream.Event do
    use Signal.Type

    alias Signal.Codec
    alias Signal.Stream.Event

    schema enforce: true do
        field :uuid,            String.t()
        field :topic,           String.t()
        field :stream_id,       String.t()
        field :position,        integer(),      default: 0
        field :data,            map()
        field :timestamp,       term()
        field :causation_id,    String.t(),     default: nil
        field :correlation_id,  String.t(),     default: nil
    end

    def new(data, opts) when is_struct(data) and is_list(opts) do
        {:ok, payload} = Codec.encode(data)
        uuid = UUID.uuid4()
        opts
        |> Keyword.put(:data, payload)
        |> Keyword.put_new(:uuid, uuid)
        |> Keyword.put_new(:causation_id, uuid)
        |> Keyword.put_new(:correlation_id, uuid)
        |> Keyword.put_new_lazy(:topic, fn -> 
            Signal.Name.name(data)
        end)
        |> Keyword.put_new_lazy(:timestamp, fn -> 
            DateTime.utc_now()
        end)
        |> Keyword.put_new_lazy(:stream_id, fn -> 
            Signal.Stream.id(data)
        end)
        |> new()
    end

    def payload(%Event{}=event) do
        Signal.Event
        |> struct(Map.from_struct(event))
        |> Signal.Event.data()
    end

end
