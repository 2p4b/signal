defmodule Signal.Projector do

    alias Signal.Projector
    alias Signal.Stream.Event

    defstruct [:app, :name, :module, :subscription]

    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        start = Keyword.get(opts, :start, :current)
        quote do
            use GenServer
            alias Signal.Projector
            alias Signal.Events.Event

            @app unquote(app)

            @signal_start unquote(start)

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
                opts = [
                    application: @app, 
                    topics: @topics, 
                    name: @name, 
                    start: @signal_start
                ] ++ opts 
                GenServer.start_link(__MODULE__, opts, name: __MODULE__)
            end

            @impl true
            def init(opts) do
                Projector.init(__MODULE__, opts)
            end

            @impl true
            def handle_info(%Event{}=event, %Projector{}=projector) do
                Projector.handle_event(projector, event)
            end

        end
    end


    def init(module, opts) do
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        application = Keyword.get(opts, :application)
        start = Keyword.get(opts, :start, :current)
        tenant = Keyword.get(opts, :tenant, application)
        app = {application, tenant}
        sub_opts = [
            start: start,
            topics: topics, 
            tenant: tenant,
        ]
        {:ok, sub} = subscribe(app, name, sub_opts)
        params = [name: name, app: app, module: module, subscription: sub]
        {:ok, struct(__MODULE__, params)} 
    end

    def subscribe(app, name, opts) do
        {application, _tenant} = app
        Enum.find_value(1..5, fn _x -> 
            case application.subscribe(name, opts) do
                {:ok, subscription} ->
                    {:ok, subscription}
                _ ->
                    Process.sleep(50)
                    false
            end
        end)
    end

    def handle_event(%Projector{}=projector, %Event{number: number}=event) do
        %Projector{
            app: app, 
            module: module, 
            subscription: %{handle: handle}
        } = projector
        {application, tenant} = app
        args = [Event.payload(event), Event.metadata(event)]
        response = Kernel.apply(module, :project, args)
        application.acknowledge(handle, number, tenant: tenant)
        handle_response(projector, response)
    end

    def handle_response(%Projector{}=handler, response) do
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

