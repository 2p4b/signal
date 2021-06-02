defprotocol Signal.Identifier do

    @fallback_to_any true
    @spec identify(t) :: String.t()
    def identify(command)

end

defimpl Signal.Identifier, for: Any do

    def identify(%{uuid: id}), do: id

    def identify(%{id: id}), do: id
    
end


