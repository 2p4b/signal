defmodule Signal.Stream.History do
    @enforce_keys [:stream, :position, :events]
    defstruct [:stream, :position, :events]
end

