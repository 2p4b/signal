defprotocol Signal.Name do

    @fallback_to_any true

    @spec name(t) :: String.t()
    def name(event)

end

defimpl Signal.Name, for: Any do
    def name(%{__struct__: type}) when is_atom(type) do
        Signal.Helper.module_to_string(type)
    end
end




