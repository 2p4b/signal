defmodule Signal.Effect do
    defstruct [:uuid, :id, :namespace, :object, :number]

    def new(opts) when is_list(opts) do
        uuid = 
            opts 
            |> Enum.into(%{}) 
            |> create_uuid()
        struct(__MODULE__, Keyword.merge(opts, [uuid: uuid]))
    end

    defp create_uuid(%{namespace: namespace, id: id}) do
        :oid
        |> UUID.uuid5(namespace)
        |> UUID.uuid5(id)
    end


end
