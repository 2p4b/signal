defmodule Signal.Process do
    
    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        stop_timeout = Keyword.get(opts, :stop_timeout, 100)

        quote location: :keep do

            use GenServer
            use Blueprint.Struct
            alias Signal.Helper
            alias Signal.Stream.Event
            alias Signal.Process.Router
            @before_compile unquote(__MODULE__)

            @stop_timeout unquote(stop_timeout)

            @app unquote(app)

            @name (if unquote(name) do 
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
                    name: @name,
                    topics: @topics,
                    module: __MODULE__,
                    application: @app,
                ] 
                Router.init(params ++ opts)
            end


            @impl true
            def handle_info(:boot, router) do
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
            def handle_cast({:ack, id, number, status}, router) do
                Router.handle_ack({status, id, number}, router)
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
