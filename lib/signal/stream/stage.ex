defmodule Signal.Stream.Stage do
    @enforce_keys [:stream, :events]
    defstruct [:stream, :position, :events, :stage]
end
