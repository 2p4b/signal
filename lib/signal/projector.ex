defmodule Signal.Projector do

    alias Signal.Projector
    alias Signal.Events.Event
    alias Signal.Subscription
    alias Signal.Channels.Channel

    defstruct [:app, :name]

    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        quote do
            use GenServer
            alias Signal.Projector
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
                Projector.init(__MODULE__, opts)
            end

            @impl true
            def handle_info(%Event{}=event, %Projector{}=handler) do
                Projector.handle_event(__MODULE__, event, handler)
            end

        end
    end


    def init(module, opts) do
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        application = Keyword.get(opts, :application)
        app_name = Keyword.get(opts, :app, application)
        app = {application, app_name}
        %Subscription{} = Channel.subscribe(app, name, topics)
        params = [name: name, app: app]
        {:ok, struct(__MODULE__, params)} 
    end

    def handle_event(module, event, handler) do
        %Event{payload: payload, number: number} = event
        %Projector{app: app, name: name} = handler
        response = Kernel.apply(module, :project, [payload, Event.meta(event)])
        Channel.acknowledge(app, name, number)
        handle_response(response, handler)
    end

    def handle_response(response, %Projector{}=handler) do
        case response do
            :stop ->
                {:stop, :stopped, handler}

            {:stop, reason} ->
                {:stop, reason, handler}

            :hibernate ->
                {:noreply, handler, :hibernate}

            _resp ->
                {:noreply, handler}
        end
    end

end

