defmodule Signal.Process do
    
    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        timeout = Keyword.get(opts, :timeout, 5000)

        quote location: :keep do

            use GenServer
            use Blueprint.Struct
            alias Signal.Helper
            alias Signal.Event
            alias Signal.Process.Router
            @before_compile unquote(__MODULE__)

            @timeout unquote(timeout)

            @app unquote(app)

            @name (if not(is_nil(unquote(name))) and is_binary(unquote(name)) do 
                unquote(name) 
            else 
                Signal.Helper.module_to_string(__MODULE__) 
            end)

            @topics (unquote(topics) |> Enum.map(fn 
                topic when is_binary(topic) -> topic 
                topic when is_atom(topic) -> Signal.Helper.module_to_string(topic)
            end))

            @doc """
            Starts a new execution queue.
            """
            def start_link(opts) do
                GenServer.start_link(__MODULE__, opts, name: __MODULE__)
            end

            @impl true
            def init(opts) when is_list(opts) do
                params = [
                    app: @app,
                    name: @name,
                    topics: @topics,
                    timeout: @timeout,
                    module: __MODULE__,
                ] 
                Router.init(params ++ opts)
            end


            @impl true
            def handle_continue(:boot, router) do
                Router.handle_boot(router)
            end

            @impl true
            def handle_info({:next, id}, router) do
                Router.handle_next(id, router)
            end

            @impl true
            def handle_info({:DOWN, ref, :process, _obj, _rsn}, router) do
                Router.handle_down(ref, router)
            end

            @impl true
            def handle_info(%Event{}=event, router) do
                Router.handle_event(event, router)
            end

            @impl true
            def handle_info({:stop, id}, router) do
                Router.handle_stop(id, router)
            end

            @impl true
            def handle_info({:start, id, number}, router) do
                Router.handle_start({id, number}, router)
            end

            @impl true
            def handle_info({:ack, id, number, status}, router) do
                Router.handle_ack({id, number}, router)
            end

            @impl true
            def handle_info(:timeout, router) do
                Router.handle_timeout(router)
            end

            @impl true
            def handle_call({:alive, id}, _from, router) do
                Router.handle_alive(id, router)
            end

        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true do
            def handle(_event), do: :skip
        end
    end
end
