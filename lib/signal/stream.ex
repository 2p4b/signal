defprotocol Signal.Stream do

    @spec stream(t, o::list) :: {String.t(), atom()}
    def stream(entity, opts\\[])

    @spec id(t, o::list) :: String.t()
    def id(entity, opts\\[])

    @spec type(t, o::list) :: atom()
    def type(entity, opts\\[])

    @spec tag(t, o::list) :: atom()
    def tag(entity, opts\\[])

    @spec stream(t, o::list) :: {atom(), String.t()}
    def iden(entity, opts\\[])

end




