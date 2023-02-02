defprotocol Signal.Stream.Reducer do
    @spec apply(aggregate::term(), event::term()) :: term()
    def apply(aggregate, event)
end

