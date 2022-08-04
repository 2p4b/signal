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
                        def stream(command, _res) do 
                            id = Map.get(command, @field)
                            @stream_module
                            |> Signal.Helper.stream_tuple(id, @stream_opts)
                        end
                    else
                        def stream(_command, _res) do 
                            id = @field
                            @stream_module
                            |> Signal.Helper.stream_tuple(id, @stream_opts)
                        end
                    end
                end
            end
        end
    end
end
