defprotocol Signal.Stream do

    @spec stream(t, r :: map) :: {atom(), String.t()}
    def stream(entity, res \\ nil)

end




