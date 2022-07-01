defprotocol Signal.Stream do

    @spec stream(t, r :: map) :: {String.t(), atom()}
    def stream(entity, res \\ nil)

end




