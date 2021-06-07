defmodule Signal.Exception do

    defmodule StreamError do
        defexception [:stream, :message]

        @impl true
        def message(%{stream: {type, id}, message: message}) when is_binary(message) do
            """
            StreamError
                type: #{inspect(type)}
                  id: #{inspect(id)}

            #{message}
            """
        end

        def message(exception) do
            """
            StreamError
            stream: #{inspect(exception)}
            """
        end
    end

end
