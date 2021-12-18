defprotocol Signal.Sync do

    @fallback_to_any true

    @spec sync(t, r :: term()) :: boolean()
    def sync(entity, results)

end

defimpl Signal.Sync, for: Any do
    def sync(_commmand, _results), do: false
end
