defmodule Signal.Aggregate do

    defmacro __using__(opts) do
        strict = Keyword.get(opts, :strict, false)
        quote do
            use Blueprint.Struct
            @module __MODULE__
            @apply_strict unquote(strict)
            @before_compile unquote(__MODULE__)
        end
    end

    defmacro __before_compile__(_env) do

        quote generated: true, location: :keep do
            if Module.defines?(__MODULE__, {:apply, 3}, :def) do
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

end



