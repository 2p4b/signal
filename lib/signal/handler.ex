defmodule Signal.Handler do

    alias Signal.Handler
    alias Signal.Logger
    alias Signal.Stream.Event

    defstruct [:app, :state, :module, :subscription]

    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        start = Keyword.get(opts, :start, :current)
        quote do
            use GenServer, restart: :transient
            alias Signal.Handler
            alias Signal.Stream.Event

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
                    name: @name,
                    start: @signal_start,
                    topics: @topics, 
                    application: @app
                ] ++ opts 
                GenServer.start_link(__MODULE__, opts, name: __MODULE__)
            end

            @impl true
            def init(opts) do
                Handler.init(__MODULE__, opts)
            end

            @impl true
            def handle_info(%Event{}=event, %Handler{}=handler) do
                Handler.handle_event(handler, event)
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
        start = Keyword.get(opts, :start, :current)
        application = Keyword.get(opts, :application)
        tenant = Keyword.get(opts, :tenant, application)
        app = {application, tenant}
        {:ok, subscription} = subscribe(app, name, topics, start)
        init_params = []
        case Kernel.apply(module, :init, [subscription, init_params]) do
            {:ok, state} ->
                params = [state: state, app: app, subscription: subscription, module: module]
                {:ok, struct(__MODULE__, params)} 
            error -> 
                error
        end
    end

    def subscribe(app, name, topics, start \\ :current) do
        {application, tenant} = app
        opts = [topics: topics, tenant: tenant, start: start]
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


    def handle_event(handler, event) do
        %Event{number: number} = event
        %Handler{
            app: app, 
            module: module, 
            state: state, 
            subscription: %{handle: handle}
        } = handler
        {application, tenant} = app

        [
          processing: event.topic,
          topic: event.topic,
          number: event.number,
        ]
        |> Logger.info(info, label: :handler)

        args = [Event.payload(event), Event.metadata(event), state]

        response =
            case Kernel.apply(module, :handle_event, args) do
                {:error, _reason, _}=error  ->
                    error

                {:error, _}=error ->
                    error

                response when not is_tuple(response) ->
                    application.acknowledge(handle, number, tenant: tenant)
                    {:noreply, response}


               response -> 
                    application.acknowledge(handle, number, tenant: tenant)
                    response
            end

        handle_response(response, handler)
    end

    def handle_response(response, %Handler{}=handler) do
        case response do
            {:noreply, state} ->
                {:noreply, %Handler{handler | state: state}}

            {:stop, state} ->
                {:stop, nil, state}

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

