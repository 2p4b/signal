defmodule Signal.Projector do
    defmacro __using__(opts) do
        opts = 
            opts
            |> Keyword.put(:__using__, Signal.Projector)
            |> Keyword.put_new_lazy(:start, fn -> :beginning end)
        quote [location: :keep, line: 3] do
            require Signal.Handler
            Signal.Handler.__using__(unquote(opts))

            def init(opts) do
                {:ok, opts}
            end

            def handle_event(event, opts) do
                Kernel.apply(__MODULE__, :project, [event])
                {:noreply, opts}
            end

        end
    end
end

