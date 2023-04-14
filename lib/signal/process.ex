defmodule Signal.Process do
    
    defmacro __using__(opts) do
        app = Keyword.get(opts, :app)
        name = Keyword.get(opts, :name)
        start = Keyword.get(opts, :start)
        topics = Keyword.get(opts, :topics)
        timeout = Keyword.get(opts, :timeout, 5000)
        restart = Keyword.get(opts, :restart, :transient)
        shutdown = Keyword.get(opts, :shutdown, 1)

        quote location: :keep do

            use Blueprint.Struct

            @signal__process__app unquote(app)

            @signal__process__start unquote(start)

            @signal__process__timeout unquote(timeout)

            @signal__process__restart unquote(restart)

            @signal__process__shutdown unquote(shutdown)

            if is_nil(@signal__process__app) or not(is_atom(@signal__process__app)) do
                Signal.Exception.raise_invalid_app(__MODULE__, Signal.Process)
            end

            @signal__process__name (if not(is_nil(unquote(name))) and is_binary(unquote(name)) do 
                unquote(name) 
            else 
                Signal.Helper.module_to_string(__MODULE__) 
            end)

            @signal__process__topics (unquote(topics) |> Enum.map(fn 
                topic when is_binary(topic) -> topic 
                topic when is_atom(topic) -> Signal.Helper.module_to_string(topic)
            end))


            def start_link(opts) do
                GenServer.start_link(Signal.Process.Router, opts, name: __MODULE__)
            end

            def child_spec(opts) do
                opts = [
                    app: @signal__process__app,
                    name: @signal__process__name,
                    module: __MODULE__,
                    start: @signal__process__start,
                    topics: @signal__process__topics, 
                    timeout: @signal__process__timeout,
                ] 
                |> Keyword.put(:opts, opts)

                %{
                    id: __MODULE__,
                    start: {__MODULE__, :start_link, [opts]},
                    restart: @signal__process__restart,
                    shutdown: @signal__process__shutdown
                }
            end

        end
    end

end
