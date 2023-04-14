defmodule Signal.Handler do
    use GenServer
    alias Signal.Event
    alias Signal.Logger
    alias Signal.Handler

    defstruct [:app, :state, :module, :consumer]

    defmacro __using__(opts) do
        using = Keyword.get(opts, :__using__, __MODULE__)
        app = Keyword.get(opts, :app)
        name = Keyword.get(opts, :name)
        start = Keyword.get(opts, :start)
        topics = Keyword.get(opts, :topics)
        restart = Keyword.get(opts, :restart, :transient)
        shutdown = Keyword.get(opts, :shutdown, 1)

        quote do
            alias Signal.Event
            alias Signal.Handler

            @signal__handler__app unquote(app)

            @signal__handler__restart unquote(restart)

            @signal__handler__shutdown unquote(shutdown)

            if is_nil(@signal__handler__app) or not(is_atom(@signal__handler__app)) do
                Signal.Exception.raise_invalid_app(__MODULE__, unquote(using))
            end

            @signal_handler_start unquote(start)

            @signal_handler_name (if unquote(name) do 
                unquote(name) 
            else 
                Signal.Helper.module_to_string(__MODULE__) 
            end)

            @signal_handler_topics (unquote(topics) |> Enum.map(fn 
                topic when is_binary(topic) -> topic 
                topic when is_atom(topic) -> Signal.Helper.module_to_string(topic)
            end))

            @doc """
            Starts a new execution queue.
            """
            def start_link(opts) do
                GenServer.start_link(Signal.Handler, opts, name: __MODULE__)
            end

            def child_spec(opts) do
                opts = [
                    app: @signal__handler__app,
                    name: @signal_handler_name,
                    module: __MODULE__,
                    start: @signal_handler_start,
                    topics: @signal_handler_topics, 
                ] 
                |> Keyword.put(:opts, opts)

                %{
                    id: __MODULE__,
                    start: {__MODULE__, :start_link, [opts]},
                    type: :worker,
                    restart: @signal__handler__restart,
                    shutdown: @signal__handler__shutdown
                }
            end

        end
    end


    @impl true
    def init(opts) do
        app = Keyword.fetch!(opts, :app)
        name = Keyword.fetch!(opts, :name)
        start = Keyword.get(opts, :start)
        topics = Keyword.fetch!(opts, :topics)
        module = Keyword.fetch!(opts, :module)
        init_opt = Keyword.fetch!(opts, :opts)

        case Kernel.apply(module, :init, [init_opt]) do
            {:ok, state} ->
                consumer = subscribe(app, name, topics, start)
                params = [state: state, app: app, consumer: consumer, module: module]
                {:ok, struct(__MODULE__, params)} 

            error -> 
                error
        end
    end

    @impl true
    def handle_info(%Event{}=event, %Handler{}=handler) do
        %Event{number: number} = event
        %Handler{
            app: app, 
            state: state, 
            consumer: consumer,
        } = handler

        [
            app: app,
            handler: handler.module,
            processing: event.topic,
            topic: event.topic,
            number: event.number,
        ]
        |> Logger.info(label: :handler)

        args = [Event.data(event), state]

        response =
            handler.module
            |> Kernel.apply(:handle_event, args)
            |> handle_response(handler) 

        Signal.Event.Broker.acknowledge(app, consumer, number)

        response
    end

    @impl true
    def handle_info(request, %Handler{state: state}=handler) do
        handler.module
        |> Kernel.apply(:handle_info, [request, state])
        |> Handler.handle_response(handler)
    end

    @impl true
    def handle_continue(request, %Handler{state: state}=handler) do
        handler.module
        |> Kernel.apply(:handle_continue, [request, state])
        |> Handler.handle_response(handler)
    end

    @impl true
    def handle_cast(request, %Handler{state: state}=handler) do
        handler.module
        |> Kernel.apply(:handle_cast, [request, state])
        |> Handler.handle_response(handler)
    end

    @impl true
    def handle_call(request, from, %Handler{state: state}=handler) do
        handler.module
        |> Kernel.apply(:handle_call, [request, from, state])
        |> Handler.handle_reply(handler)
    end

    def subscribe(app, handle, topics, start) do
        opts = [topics: topics, start: start]
        Signal.Event.Broker.subscribe(app, handle, opts)
    end

    def handle_response({:noreply, state}, %Handler{}=handler) do
        {:noreply, %Handler{handler | state: state}}
    end

    def handle_response({:noreply, state, :hibernate}, %Handler{}=handler) do
        {:noreply, %Handler{handler | state: state}, :hibernate}
    end

    def handle_response({:noreply, state, number}, %Handler{}=handler) 
    when is_integer(number) do
        {:noreply, %Handler{handler | state: state},  number}
    end

    def handle_response({:noreply, state, {:continue, args}}, %Handler{}=handler) do
        {:noreply, %Handler{handler | state: state},  {:continue, args}}
    end

    def handle_response({:stop, reason, state}, %Handler{}=handler) do
        {:stop, reason, %Handler{handler | state: state}}
    end


    # reply handle 
    def handle_reply({:reply, reply, state}, %Handler{}=handler) do
        {:reply, reply, %Handler{handler | state: state}}
    end

    def handle_reply({:reply, reply, state, :hibernate}, %Handler{}=handler) do
        {:reply, reply, %Handler{handler | state: state}, :hibernate}
    end

    def handle_reply({:reply, reply, state, number}, %Handler{}=handler) 
    when is_integer(number) do
        {:reply, reply, %Handler{handler | state: state},  number}
    end

    def handle_reply({:reply, reply, state, {:continue, args}}, %Handler{}=handler) do
        {:reply, reply, %Handler{handler | state: state},  {:continue, args}}
    end

    def handle_reply({:stop, reason, reply, state}, %Handler{}=handler) do
        {:stop, reason, reply, %Handler{handler | state: state}}
    end

    def handle_reply({:noreply, state}, %Handler{}=handler) do
        {:noreply, %Handler{handler | state: state}}
    end

    def handle_reply({:noreply, state, :hibernate}, %Handler{}=handler) do
        {:noreply, %Handler{handler | state: state}, :hibernate}
    end

    def handle_reply({:noreply, state, number}, %Handler{}=handler) 
    when is_integer(number) do
        {:noreply, %Handler{handler | state: state},  number}
    end

    def handle_reply({:noreply, state, {:continue, args}}, %Handler{}=handler) do
        {:noreply, %Handler{handler | state: state},  {:continue, args}}
    end

    def handle_reply({:stop, reason, state}, %Handler{}=handler) do
        {:stop, reason, %Handler{handler | state: state}}
    end

end

