defprotocol Signal.Topic do

    @fallback_to_any true

    @spec topic(t) :: String.t()
    def topic(event)

end

defimpl Signal.Topic, for: Any do

    def topic(%{__struct__: type}) when is_atom(type) do
        Signal.Helper.module_to_string(type)
    end

end




