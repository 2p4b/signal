defmodule Signal.Sample.Event do
    use Signal.Event,
        stream: {Signal.Sample.Aggregate, :stream_id}

    schema do
        field :value,     :number,   default: 1
        field :stream_id, :string,   default: "sample.event.id"
    end
end
