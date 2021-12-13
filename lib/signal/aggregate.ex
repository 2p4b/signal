defmodule Signal.Aggregate do

    defmacro __using__(_opts) do
        quote do
            use Blueprint.Struct
            @module __MODULE__
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
        end
    end

end



