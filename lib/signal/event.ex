defmodule Signal.Event do

    defmacro __using__(opts) do
        quote do
            use Signal.Type
            @module __MODULE__
            @before_compile unquote(__MODULE__)
            @topic Keyword.fetch(unquote(opts), :topic)
            @stream Keyword.get(unquote(opts), :stream)
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
