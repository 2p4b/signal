defprotocol Signal.Commands.Aggregate do

    @fallback_to_any true
    @spec identify(t) :: String.t()
    def identify(command)

end

defimpl Signal.Commands.Aggregate, for: Any do

    def id(_command) do
        nil
    end

    def identify(%{id: id}), do: id
    
end




