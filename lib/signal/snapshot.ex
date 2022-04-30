defmodule Signal.Snapshot do
    alias Signal.Snapshot
    defstruct [:id, :version, :data, type: nil]

    def new(id, data, opts \\ [])

    def new({type, id}, data, opts) do
        version = Keyword.get(opts, :version, 0)
        %Snapshot{
            id: id,
            type: type,
            data: data,
            version: version
        }
    end

    def new(id, data, opts) do
        new({nil, id}, data, opts)
    end

end
