defprotocol Signal.Command.Handler do

    @spec execute(t, r::map) :: {atom(), term}
    @fallback_to_any true
    def execute(command, params)

    @fallback_to_any true
    @spec handle(t :: term(), params :: term(), aggregate :: term()) :: list() | term()
    def handle(command, params, agggregate)

end

defimpl Signal.Command.Handler, for: Any do

    def execute(command, params) when is_struct(command) do
        {:ok, params}
    end

    def handle(%{__struct__: type}, _params, %{__struct__: atype}) do

        raise UndefinedFunctionError, message: """

            Undefined application command handler for #{inspect(type)}

            Ensure command implements the Signal.Command.Handler
            protocol like

            defimpl Signal.Command.Handler, for: #{inspect(type)} do
                # return event or list of events to be
                # to be dispatched
                def handle(%#{inspect(type)}{}, params, %#{inspect(atype)}{}) do
                    ...
                end
            end

            Or you use the Signal.Command command builder
            and declear the handler right from the
            same module like

            defmodule #{inspect(type)} do
                use Signal.Command

                schema do
                    ...
                end

                # return event or list of events to be
                # to be dispatched
                def handle(%#{inspect(type)}{}, params, %#{inspect(atype)}{}) do
                    ...
                end

            end
        """
    end

end
