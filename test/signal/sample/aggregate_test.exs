defmodule Signal.Sample.Aggregate do

    use Signal.Type

    schema do
        field :id,      String.t,   default: "command.id"
        field :uuid,    String.t,   default: "command.uuid"
    end

end

