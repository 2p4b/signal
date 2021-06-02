defmodule Signal.Log do
    defstruct [
        cursor: 0,
        states: [],
        params: nil,
        result: nil,
        streams: [],
        command: nil,
        indices: [],
    ]
end
