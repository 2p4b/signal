defmodule Signal.Stream.Stage do
    @enforce_keys [:stream, :events]
    defstruct [:stream, :version, :events, :stage]
end
