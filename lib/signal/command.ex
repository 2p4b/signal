defmodule Signal.Command  do

    defmacro __using__(opts) do
        quote do
            use Blueprint.Struct
            import Signal.Command
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @version Keyword.get(unquote(opts), :version)
            @queue_specs Keyword.get(unquote(opts), :queue)
            @stream_opts Keyword.get(unquote(opts), :stream)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do

            with field when is_atom(field) <- Module.get_attribute(__MODULE__,:queue) do
                defimpl Signal.Queue, for: __MODULE__ do
                    @field field
                    def queue(command) do 
                        Map.get(command, @field)
                    end
                end
            end

            with {stream_mod, field} <- Module.get_attribute(__MODULE__,:stream_opts) do
                defimpl Signal.Stream, for: __MODULE__ do
                    @field field
                    @stream_module stream_mod
                    def stream(command, _res) do 
                        {@stream_module, Map.get(command, @field)}
                    end
                end
            end

            handler_impled = Module.defines?(__MODULE__, {:handle, 3}, :def)

            executor_impled= Module.defines?(__MODULE__, {:execute, 2}, :def)

            if handler_impled and executor_impled  do
                with module <- @module do
                    defimpl Signal.Command.Handler do
                        @pmodule module
                        def execute(cmd, params) do 
                            Kernel.apply(@pmodule, :execute, [cmd, params])
                        end

                        def handle(cmd, meta, aggr) do 
                            Kernel.apply(@pmodule, :handle, [cmd, meta, aggr])
                        end
                    end
                end
            end

            if handler_impled and not(executor_impled) do
                with module <- @module do
                    defimpl Signal.Command.Handler do
                        @pmodule module
                        def execute(_cmd, params) do 
                            {:ok, params}
                        end

                        def handle(cmd, meta, aggr) do 
                            Kernel.apply(@pmodule, :handle, [cmd, meta, aggr])
                        end
                    end
                end
            end

        end
    end

end
