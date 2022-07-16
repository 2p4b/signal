defmodule Signal.Transaction do
    defstruct [
        uuid: nil,
        staged: [],
        handles: [],
        snapshots: [],
    ]

    def new(staged, opts \\ [])

    def new(staged, opts) when is_struct(staged) do
        new(List.wrap(staged), opts)
    end

    def new(staged, opts) do
        handles = Keyword.get(opts, :handles, [])
        snapshots = Keyword.get(opts, :snapshots, [])
        %Signal.Transaction{
            uuid: UUID.uuid4(),
            staged: staged,
            handles: handles,
            snapshots:  snapshots
        }
    end


end

