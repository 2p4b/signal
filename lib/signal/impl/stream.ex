defmodule Signal.Impl.Stream do
    defmacro impl(impl_opts) do
        quote do
            with stream_opts <- unquote(impl_opts) do
                {stream_mod, field, opts} =
                    case stream_opts do 
                        {module, field} ->
                            {module, field, []}

                        {module, field, opts} when is_list(opts) ->
                            {module, field, []}

                        _ ->
                            raise """
                            Compiler Error steam must be tuple/2 or tuple/3 see doc
                            """
                    end

                defimpl Signal.Stream, for: __MODULE__ do
                    @field field
                    @stream_opts opts
                    @stream_module stream_mod
                    if is_atom(@field) do
                        def id(command, opts\\[]) do
                            opts = [context: command] ++ opts ++ @stream_opts
                            command
                            |> Map.get(@field)
                            |> Signal.Helper.stream_id(opts)
                        end
                    else
                        def id(command, opts\\[]) do
                            opts = [context: command] ++ opts ++ @stream_opts
                            Signal.Helper.stream_id(@field, opts)
                        end
                    end

                    def type(command, _res\\[]) do
                        @stream_module
                    end

                    def stream(command, opts\\[]) do
                        {
                            Signal.Stream.id(command, opts), 
                            Signal.Stream.type(command, opts)
                        }
                    end

                end
            end
        end
    end
end
