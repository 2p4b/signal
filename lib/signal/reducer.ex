defprotocol Signal.Reducer do
    
    @spec reduce(t::term(), event::term(), reduction::integer) :: term()
    def reduce(t, event, reduction)

end

