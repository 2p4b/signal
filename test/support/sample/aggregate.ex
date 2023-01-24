defmodule Signal.Sample.Aggregate do

    use Signal.Aggregate

    schema do
        field :id,      :string,  default: "aggregate.id"
        field :data,    :any,     default: 0
    end

end

