defprotocol Signal.Stream.Reducer do
    
    @spec apply(aggregate::term(), meta::term(), event::term()) :: term()
    def apply(aggregate, meta, event)

end

