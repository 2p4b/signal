defmodule Signal.Effect do
    defstruct [:id, :namespace, :object, :number]

    def new(opts) do
        struct(__MODULE__, opts)
    end

end
