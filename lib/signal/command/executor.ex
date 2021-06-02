defprotocol Signal.Command.Executor do
    @spec execute(t, r::map) :: {atom(), term}
    @fallback_to_any true
    def execute(command, params)
end

defimpl Signal.Command.Executor, for: Any do
    def execute(_command, params) do
        {:ok, params}
    end
end


