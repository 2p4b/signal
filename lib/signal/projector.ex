defmodule Signal.Projector do
    defmacro __using__(opts) do
        quote [location: :keep, line: 3] do
            import Signal.Handler
            Signal.Handler.__using__(unquote(opts))

            def init(_, opts) do
                {:ok, opts}
            end

            def handle_event(event, opts) do
                Kernel.apply(__MODULE__, :project, [event])
                {:noreply, opts}
            end

        end
    end
end

