defmodule Signal.Subscription do
    defstruct [:channel, :consumer, :topics, :ack, :syn]
end

