defprotocol Signal.Codec do

    @fallback_to_any true

    @spec encode(t) :: String.t()
    def encode(type)

    @spec load(t, p :: map) :: term()
    def load(type, payload)

end

defimpl Signal.Codec, for: Any do

    def encode(data) when is_struct(data) do
        data
        |> Map.from_struct()
    end

    def load(data, payload) do
        data
        |> Map.from_struct()
        |> Map.keys()
        |> Enum.reduce(data, fn key, data -> 
            cond do 
                Map.has_key?(payload, key) ->
                    value = Map.get(payload, key)
                    Map.put(data, key, value)

                Map.has_key?(payload, to_string(key)) ->
                    value = Map.get(payload, to_string(key))
                    Map.put(data, key, value)

                true ->
                    data
            end
        end)
    end
    
end



