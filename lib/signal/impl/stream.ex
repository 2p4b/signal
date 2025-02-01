defmodule Signal.Impl.Stream do
    defmacro impl(impl_opts) do
        quote do
            with stream_opts <- unquote(impl_opts) do
                {tag, field, opts} =
                    case stream_opts do 
                        {tag, field, module} when is_atom(tag) and (is_atom(field) or is_binary(field)) and (is_atom(module) or is_list(module)) ->
                            if is_atom(module) do
                                {tag, field, type: module}
                            else
                                {tag, field, module}
                            end

                        {field, module} when (is_atom(field) or is_binary(field)) and (is_atom(module) or is_list(module)) ->
                            if is_atom(module) do
                                {nil, field, type: module}
                            else
                                {nil, field, module}
                            end

                        {stream_id, module} when is_binary(stream_id) and (is_atom(module) or is_list(module)) ->
                            if is_atom(module) do
                                {nil, stream_id, type: module}
                            else
                                {nil, stream_id, module}
                            end

                        _ ->
                            raise """
                                Invalid stream must be tuple/2 or tuple/3 see doc
                                    {:field, Module} or 
                                    {"stream_id", Module} or 
                                    {"stream_id", type: Module} or 
                                    {:namespace, :field, type: Module} or 
                                    {:namespace, "stream_id", Module} or 
                                    {:namespace, "stream_id", type: Module}
                            """
                    end

                defimpl Signal.Stream, for: __MODULE__ do
                    @tag tag
                    @field field
                    @opts opts
                    cond do

                        is_atom(@tag) and is_atom(@field) -> 
                            def iden(ctx, opts\\[]) do
                                field_id = Map.get(ctx, @field)
                                field_id = 
                                    cond do
                                        is_binary(field_id) ->
                                            field_id

                                        is_atom(field_id) ->
                                            Signal.Helper.atom_to_string(field_id)

                                        true ->
                                            raise """
                                                Invalid stream id at #{inspect(@tag)} for struct
                                                    #{inspect(ctx)}
                                            """
                                    end
                                {@tag, field_id}
                            end

                        is_atom(@tag) and is_binary(@field) ->
                            def iden(ctx, opts\\[]) do
                                {@tag, @field}
                            end

                        true ->
                            raise """
                                Invalid stream must be tuple/2 or tuple/3 see doc
                                    {:field, Module} or 
                                    {"stream_id", Module} or 
                                    {"stream_id", type: Module} or 
                                    {:namespace, :field, type: Module} or 
                                    {:namespace, "stream_id", Module} or 
                                    {:namespace, "stream_id", type: Module}
                            """
                    end

                    def id(ctx, opts\\[]) do
                        ctx
                        |> Signal.Stream.iden(opts)
                        |> Signal.Helper.stream_id(opts ++ @opts)
                    end

                    def tag(_ctx, _opts\\[]) do
                        @tag
                    end

                    def type(_ctx, _res\\[]) do
                        Keyword.fetch!(@opts, :type)
                    end

                    def stream(ctx, opts\\[]) do
                        {Signal.Stream.id(ctx, opts), Signal.Stream.type(ctx, opts)}
                    end

                end
            end
        end
    end
end
