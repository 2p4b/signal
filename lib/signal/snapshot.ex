defmodule Signal.Snapshot do
    alias Signal.Snapshot
    defstruct [:uuid, :id, :version, :data, type: nil]

    def new(id, data, opts \\ [])

    def new({id, type}, data, opts) when is_binary(id) do
        vsn = Keyword.get(opts, :version, 1)
        %Snapshot{
            id: id,
            uuid: uuid(id, type, vsn),
            type: type,
            data: data,
            version: vsn
        }
    end

    def new(id, data, opts) do
        new({id, nil}, data, opts)
    end

    def uuid(id, type, vsn) do
        case type do
            nil ->
              UUID.uuid5(:url, "#{id}:#{vsn}")
            _ ->
              UUID.uuid5(:url, "#{type}:#{id}:#{vsn}")
        end
    end

end
