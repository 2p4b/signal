defmodule Signal.Exception do

    defmodule InvalidStreamError do
        defexception [:stream, :message]

        @impl true
        def message(%{stream: stream, message: msg}) when is_binary(msg) do
            message(stream) <> msg
        end

        @impl true
        def message(%{stream: stream, message: msg}) when is_nil(msg) do
            message(stream)
        end

        @impl true
        def message(stream) do
            """
            InvalidStream
                #{inspect(stream)}

            stream MUST
                - Be a tuple
                - Have a size of 2 (tuple_size/1)
                - First element a string for the stream id
                - Second element in tuple an atom (Aggregate module) 
                  which also represents the stream type
            """
        end
    end

    defmodule StreamError do
        defexception [:stream, :message]

        @impl true
        def message(%{stream: {type, id}, message: msg}) when is_nil(msg) do
            message({type, id})
        end

        @impl true
        def message(%{stream: {type, id}, message: msg}) when is_binary(msg) do
            message({type, id})
            <>
            """

            #{msg}
            """
        end

        @impl true
        def message({type, id}) do
            """
            StreamError
                type: #{inspect(type)}
                  id: #{inspect(id)}
            """
        end

        @impl true
        def message(exception) do
            """
            StreamError
                stream: #{inspect(exception)}
            """
        end
    end


    defmodule InvalidEventError do
        defexception [:event]

        @impl true
        def message(event), do: get_fullmessage(event)
            
        def get_fullmessage(%{event: event}) do
            """
            Invalid Event
            event MUST 
                - Be a struct
                - Implement Signal.Stream

            #{inspect(event)}
            """
        end

    end
end
