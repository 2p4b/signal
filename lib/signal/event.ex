defmodule Signal.Event do

    defmacro __using__(opts) do
        quote do
            use Signal.Type
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @stream Keyword.get(unquote(opts), :stream)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true, location: :keep do
            with {module, key} <- Module.get_attribute(__MODULE__, :stream) do
                defimpl Signal.Stream, for: __MODULE__ do
                    @field key
                    @stream_module module
                    def stream(command, _res) do 
                        {@stream_module, Map.get(command, @field)}
                    end
                end
            end
        end
    end

end
