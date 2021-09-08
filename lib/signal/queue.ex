defprotocol Signal.Queue do
    
    @fallback_to_any true
    @spec queue(t) :: String.t() | nil
    def queue(t)

end

defimpl Signal.Queue, for: Any do

    def queue(_cmd), do: nil
    
end
