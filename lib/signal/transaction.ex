defmodule Signal.Transaction do
    defstruct [
        uuid: nil,
        cursor: 0,
        staged: [],
        effects: [],
    ]

    def new(staged, opts \\ [])

    def new(staged, opts) when is_struct(staged) do
        new(List.wrap(staged), opts)
    end

    def new(staged, opts) do
        effects = Keyword.get(opts, :effects, [])
        %Signal.Transaction{
            uuid: UUID.uuid4(),
            staged: staged,
            effects: effects
        }
    end


end

