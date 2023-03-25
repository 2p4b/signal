defmodule Signal.Transaction do
    defstruct [
        uuid: nil,
        cursor: 0,
        staged: [],
    ]

    def new(staged, opts \\ [])

    def new(staged, opts) when is_struct(staged) do
        new(List.wrap(staged), opts)
    end

    def new(staged, _opts) when is_list(staged) do
        %Signal.Transaction{
            uuid: UUID.uuid4(),
            staged: staged,
        }
    end


end

