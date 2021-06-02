defmodule Command do

    use Signal.Command, 
        version: "v1",
        aggregate: {:uuid, Signal.Sample.Aggregate}

    schema do
        field :uuid,    String.t,   default: "command.uuid"
    end
        
    def handle(_command, _aggregate, _params) do
        []
    end

end
