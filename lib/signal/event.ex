defmodule Signal.Event do


    defmacro __using__(opts) do
        quote do
            use Blueprint.Struct
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @topic Keyword.fetch(unquote(opts), :topic)
            @stream_opts Keyword.get(unquote(opts), :stream)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do

            with {:ok, topic} <- Module.get_attribute(__MODULE__, :topic) do
                defimpl Signal.Topic, for: __MODULE__ do
                    @topic (if is_binary(topic) do 
                        topic
                    else 
                        Signal.Helper.module_to_string(topic) 
                    end)

                    def topic(_event) do 
                        @topic
                    end
                end
            end

            if Module.defines?(__MODULE__, {:apply, 3}, :def) do
                with module <- @module do
                    defimpl Signal.Stream.Reducer do
                        @pmodule module
                        def apply(event, meta, agg) do 
                            Kernel.apply(@pmodule, :apply, [event, meta, agg])
                        end
                    end
                end
            end

            with stream_opts when is_tuple(stream_opts) 
                 <- Module.get_attribute(__MODULE__, :stream_opts) do
                require Signal.Impl.Stream
                Signal.Impl.Stream.impl(stream_opts)
            end

        end
    end

end
