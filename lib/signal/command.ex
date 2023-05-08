defmodule Signal.Command  do

    defmacro __using__(opts) do
        quote do
            use Blueprint.Schema
            import Signal.Command
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @sync Keyword.get(unquote(opts), :sync)
            @queue Keyword.get(unquote(opts), :queue)
            @stream Keyword.get(unquote(opts), :stream)
            @name Keyword.get(unquote(opts), :name, __MODULE__)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do

            with {:ok, name} <- Module.get_attribute(__MODULE__, :name) do
                defimpl Signal.Name, for: __MODULE__ do
                    @name (if is_binary(name) do 
                        name
                    else 
                        Signal.Helper.module_to_string(name) 
                    end)

                    def name(_event) do 
                        @name
                    end
                end
            end

            with field when is_atom(field) 
                 <- Module.get_attribute(__MODULE__, :queue) do
                defimpl Signal.Queue, for: __MODULE__ do
                    @field field
                    def queue(command) do 
                        Map.get(command, @field)
                    end
                end
            end

            with sync when is_boolean(sync) 
                 <- Module.get_attribute(__MODULE__, :sync) do
                defimpl Signal.Sync, for: __MODULE__ do
                    @csync sync
                    def sync(_command, _results) do 
                        @csync
                    end
                end
            end

            with stream_opts when is_tuple(stream_opts) 
                 <- Module.get_attribute(__MODULE__, :stream) do
                require Signal.Impl.Stream
                Signal.Impl.Stream.impl(stream_opts)
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
