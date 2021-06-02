defmodule Signal.Events.Staged do

    @enforce_keys [:stream, :version, :events, :stage]
    defstruct [:stream, :version, :events, :stage]

end

