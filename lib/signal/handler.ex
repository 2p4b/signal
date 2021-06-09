defmodule Signal.Handler do

    alias Signal.Handler
    alias Signal.Events.Event
    alias Signal.Channels.Channel

    defstruct [:app, :name, :state]

    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        quote do
            use GenServer
            alias Signal.Handler
            alias Signal.Events.Event

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
                opts = [application: @app, topics: @topics, name: @name] ++ opts 
                GenServer.start_link(__MODULE__, opts, name: __MODULE__)
            end

            @impl true
            def init(opts) do
                Handler.init(__MODULE__, opts)
            end

            @impl true
            def handle_info(%Event{}=event, %Handler{}=handler) do
                Handler.handle_event(__MODULE__, event, handler)
            end

            @impl true
            def handle_info(request, %Handler{state: state}=handler) do
                Kernel.apply(__MODULE__, :handle_info, [request, state])
                |> Handler.handle_response(handler)
            end

            @impl true
            def handle_continue(request, %Handler{state: state}=handler) do
                Kernel.apply(__MODULE__, :handle_continue, [request, state])
                |> Handler.handle_response(handler)
            end

            @impl true
            def handle_cast(request, %Handler{state: state}=handler) do
                Kernel.apply(__MODULE__, :handle_cast, [request, state])
                |> Handler.handle_response(handler)
            end

            @impl true
            def handle_call(request, from, %Handler{state: state}=handler) do
                Kernel.apply(__MODULE__, :handle_call, [request, from, state])
                |> Handler.handle_response(handler)
            end

        end
    end


    def init(module, opts) do
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        application = Keyword.get(opts, :application)
        app_name = Keyword.get(opts, :app, application)
        app = {application, app_name}
        subscription = subscribe(app, name, topics)
        init_params = [app: app_name, name: name]

        case Kernel.apply(module, :init, [subscription, init_params]) do
            {:ok, state} ->
                params = [name: name, state: state, app: app]
                {:ok, struct(__MODULE__, params)} 
            error -> 
                error
        end
    end

    def subscribe(app, name, topics) do
        Enum.find_value(1..5, fn _x -> 
            case Channel.subscribe(app, name, topics) do
                %Signal.Subscription{} = subscription ->
                    subscription
                _ ->
                    Process.sleep(50)
                    false
            end
        end)
    end


    def handle_event(module, event, handler) do
        %Event{number: number} = event
        %Handler{app: app, name: name, state: state} = handler
        args = [Event.payload(event), Event.metadata(event), state]
        response = Kernel.apply(module, :handle_event, args)
        Channel.acknowledge(app, name, number)
        handle_response(response, handler)
    end

    def handle_response(response, %Handler{}=handler) do
        case response do
            {:noreply, state} ->
                {:noreply, %Handler{handler | state: state}}

            {:stop, reason, state} ->
                {:stop, reason, state}

            {:noreply, state, other} ->
                {:noreply, %Handler{handler | state: state}, other}

            {:reply, reply, state} ->
                {:reply, reply, %Handler{handler | state: state}}

            {:reply, reply, state, other} ->
                {:reply, reply, %Handler{handler | state: state}, other}

            resp ->
                resp
        end
    end

end

