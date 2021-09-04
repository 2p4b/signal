defmodule Signal.Events.Stage do

    @enforce_keys [:stream, :events]
    defstruct [:stream, :version, :events, :stage]

end
