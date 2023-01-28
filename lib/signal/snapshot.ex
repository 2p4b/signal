defmodule Signal.Snapshot do
    alias Signal.Snapshot
    defstruct [:id, :version, :data]

    def new(opts) when is_list(opts) do
        struct(Snapshot, opts)
    end

end
