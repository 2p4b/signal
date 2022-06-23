defprotocol Signal.Reducer do
    
    @spec reduce(t::term(), event::term(), reduction::integer) :: {atom(), term()} | {atom(), atom(), term()} | {atom(), binary(), term()}
    def reduce(t, event, reduction)

end

