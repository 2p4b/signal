defmodule Signal.Stream.Consumer do
    defstruct [:stream, :pid, :ref, :ack, :syn]
end
