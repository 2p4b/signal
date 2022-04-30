defmodule Signal.Transaction do
    alias Signal.Transaction
    defstruct [
        staged: [],
        handles: [],
        snapshots: [],
    ]

    def new(staged, opts \\ []) do
        handles = Keyword.get(opts, :handles, [])
        snapshots = Keyword.get(opts, :snapshots, [])
        %Transaction{
            staged: staged,
            handles: handles,
            snapshots:  snapshots
        }
    end
end

