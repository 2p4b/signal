defmodule Signal.Effect do
    defstruct [:uuid, :namespace, :data]

    def new(opts) when is_list(opts) do
        struct(__MODULE__, opts)
    end

    def uuid(domain, id) when is_binary(domain) and is_binary(id) do
        :oid
        |> UUID.uuid5(domain)
        |> UUID.uuid5(id)
    end

end
