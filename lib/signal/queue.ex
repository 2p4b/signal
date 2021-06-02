defprotocol Signal.Queue do
    
    @fallback_to_any true
    @spec queue(t) :: {atom, String.t()}
    def queue(t)

end

defimpl Signal.Queue, for: Any do

    def queue(_cmd), do: nil
    
end
