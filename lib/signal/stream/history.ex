defmodule Signal.Stream.History do
    @enforce_keys [:stream, :version, :events]
    defstruct [:stream, :version, :events]
end

