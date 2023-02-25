defmodule Signal.Snapshot do
    alias Signal.Snapshot
    defstruct [:id, :uuid, :version, :data]

    def uuid(stream_id, version) when is_integer(version) do
        uuid(stream_id, Integer.to_string(version))
    end

    def uuid(stream_id, version) when is_binary(version) do
        :oid
        |> UUID.uuid5(stream_id)
        |> UUID.uuid5(Integer.to_string(version))
    end

    def new(opts) when is_list(opts) do
        id = Keyword.fetch!(opts, :id)
        version = Keyword.fetch!(opts, :version)
        struct(Snapshot, Keyword.merge(opts, uuid: uuid(id, version)))
    end

end
