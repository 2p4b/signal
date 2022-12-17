defmodule Signal.Aggregate do

    defprotocol Config do

        @fallback_to_any true
        @spec config(t) :: Keyword.t()
        def config(type)

    end

    defimpl Config, for: Any do
        def config(_type) do
            [timeout: Signal.Timer.seconds(5)]
        end
    end

    defmacro __using__(opts) do
        strict = Keyword.get(opts, :strict, false)
        timeout = Keyword.get(opts, :timeout, Signal.Timer.seconds(5))
        quote do
            use Blueprint.Struct
            @module __MODULE__
            @timeout unquote(timeout)
            @apply_strict unquote(strict)
            @before_compile unquote(__MODULE__)
        end
    end

    defmacro __before_compile__(_env) do

        quote generated: true, location: :keep do

            with timeout <- @timeout do
                defimpl Signal.Aggregate.Config do
                    @timeout timeout
                    def config(_) do
                        [timeout: @timeout]
                    end
                end
            end

            if (Module.defines?(__MODULE__, {:apply, 3}, :def) or @apply_strict === false) do
                with module <- @module do
                    defimpl Signal.Stream.Reducer do
                        @pmodule module
                        def apply(agg, meta, event) do 
                            Kernel.apply(@pmodule, :apply, [event, meta, agg])
                        end
                    end
                end
            end

            if @apply_strict === false do
                def apply(_event, _meta, state) do
                    {:ok, state}
                end
            end

        end
    end

    def snapshot(aggregate, action \\  nil)
    def snapshot(aggregate, nil) do
        {:snapshot, aggregate}
    end
    def snapshot(aggregate, :sleep) do
        {:snapshot, aggregate, :sleep}
    end
    def snapshot(aggregate, [timeout: timeout]) when is_number(timeout) do
        {:snapshot, aggregate, timeout}
    end

    def sleep(aggregate, action \\  nil)
    def sleep(aggregate, nil) do
        {:sleep, aggregate}
    end
    def sleep(aggregate, [timeout: timeout]) when is_number(timeout) do
        {:sleep, aggregate, timeout}
    end

    def continue(aggregate, action \\  nil)
    def continue(aggregate, nil) do
        {:ok, aggregate}
    end
    def continue(aggregate, [timeout: timeout]) when is_number(timeout) do
        {:ok, aggregate, timeout}
    end

end

