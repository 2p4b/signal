defmodule Signal.Process.Manager do

    defstruct [:name, :store, :procs, :app, :topics, :subscription, :module]

    alias Signal.Subscription
    alias Signal.Process.Saga
    alias Signal.Events.Event
    alias Signal.Process.Manager
    alias Signal.Channels.Channel

    defmodule Proc do
        defstruct [:id, :saga, :ack, :syn]

        def new(id, pid, index \\ 0) do
            saga = {pid, Process.monitor(pid)}
            struct(__MODULE__, [id: id, saga: saga, ack: index, syn: index])
        end
    end
    
    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        stop_timeout = Keyword.get(opts, :stop_timeout, 100)

        quote location: :keep do

            use GenServer
            alias Signal.Helper
            alias Signal.Process.Manager
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
                Manager.init(params ++ opts)
            end

            @impl true
            def handle_info(:init, state) do
                Manager.handle_init(state)
            end

            @impl true
            def handle_info({:next, id}, state) do
                Manager.handle_next(id, state)
            end

            @impl true
            def handle_info({:ack, id, number}, state) do
                Manager.handle_ack({id, number}, state)
            end

            @impl true
            def handle_info({:DOWN, ref, :process, _obj, _rsn}, manager) do
                Manager.handle_down(ref, manager)
            end

            @impl true
            def handle_info(event, state) do
                Manager.handle_event(event, state)
            end

            @impl true
            def handle_call({:alive, id}, _from, state) do
                Manager.handle_alive(id, state)
            end

            def alive?(id) do
                GenServer.call(__MODULE__, {:alive, id}, 5000)
            end

        end
    end

    def init(opts) do
        Process.send(self(), :init, [])
        application = Keyword.get(opts, :application)
        app = {application, Keyword.get(opts, :app, application)}
        params = [
            app: app, 
            name: Keyword.get(opts, :name), 
            procs: [],
            module: Keyword.get(opts, :module),
            topics: Keyword.get(opts, :topics),
            store: Signal.Application.store(app), 
        ]
        {:ok, struct(__MODULE__, Keyword.merge(opts, params) )}
    end

    def handle_init(%Manager{}=state) do
        %Manager{store: store, name: name, app: app, topics: topics} = state
        {procs, opts} = 
            case store.get_state(app, name, :max) do
                {index, ids} when is_binary(ids) ->
                    procs =
                        Enum.map(ids, fn id -> 
                            pid = start_process(state, id) 
                            {:ok, ack} = resume_process(state, id, true)
                            Proc.new(id, pid, ack)
                        end)
                    {procs, [start: index]}
                _ -> {[], []}
            end
        sub = Channel.subscribe(app, name, topics, opts)
        {:noreply, %Manager{state| procs: procs, subscription: sub}}
    end

    def handle_down(ref, %Manager{procs: procs}=state) do
        procs = 
            Enum.map(procs, fn 
                %Proc{id: id, saga: {_pid, ^ref}} ->
                    Process.demonitor(ref)
                    pid = start_process(state, id) 
                    {:ok, ack} = resume_process(state, id, true)
                    Proc.new(id, pid, ack)
                proc ->
                    proc
            end)
        {:noreply, %Manager{state | procs: procs}}
    end

    def handle_next(id, %Manager{procs: procs}=state) do
        procs = 
            Enum.map(procs, fn 
                %Proc{id: ^id}=proc -> 
                    case next_process_event(proc, state) do
                        [%Event{}=event] ->
                            push_event(event, proc)
                        [] ->
                            proc
                    end

                proc -> proc
            end)
        {:noreply, %Manager{state | procs: procs}}
    end

    def handle_ack({id, number}, %Manager{ subscription: %{ack: index} }=state) do
        procs = 
            Enum.map(state.procs, fn 
                %Proc{id: ^id, syn: syn}=proc -> 
                    if syn == number and number < index do
                        Process.send(self(), {:next, id}, [])
                    end
                    %Proc{proc | ack: number}

                proc -> 
                    proc 
            end)
        {:noreply, %Manager{state | procs: procs} }
    end

    def handle_alive(id, %Manager{procs: procs}=state) do
        found =
            case Enum.find(procs, &(Map.get(&1, :id) == id))  do
                nil -> false
                _found -> true
            end
        {:reply, found, state}
    end

    def handle_event(%Event{number: number}=event, %Manager{module: module, subscription: %{ack: index}}=state) do
        procs =
            case Kernel.apply(module, :handle, [Event.payload(event)]) do

                {stop, id} when stop in [:stop, :stop!] ->
                    proc = Enum.find(state.procs, &(Map.get(&1, :id) == id))
                    if is_nil(proc) do
                        state.procs
                    else
                        {pid, ref} = proc.saga
                        case GenServer.call(pid, {:stop, event}) do
                            :continue -> 
                                state.procs

                            _ ->
                                Process.demonitor(ref)
                                Enum.filter(state.procs, &(Map.get(&1, :id) != id))
                        end
                    end

                {start, id} when start in [:start, :start!] ->
                    pid = start_process(state, id)
                    case init_process(state, id, number, start == :start!) do
                        {:ok, _state} ->
                            state.procs ++ [Proc.new(id, pid, index)]
                        _ ->
                            state.procs
                    end

                {resume, id} when resume in [:resume, :resume!] ->
                    proc = Enum.find(state.procs, &(Map.get(&1, :id) == id))
                    if is_nil(proc) do
                        pid = start_process(state, id) 
                        {:ok, ack} = 
                            resume_process(state, id, resume == :resume!)
                        state.procs ++ [Proc.new(id, pid, ack)]
                    else
                        state.procs
                    end

                :ok ->
                    state.procs
            end
            |> Enum.map(fn 
                %Proc{syn: ^index, ack: ^index}=proc -> 
                    push_event(event, proc)
                proc -> 
                    proc 
            end)

        state = acknowledge(event, state)

        if length(procs) != length(state.procs) do
            {:noreply, update_processes(state, procs)}
        else
            {:noreply, state}
        end
    end

    defp acknowledge(%Event{number: number}, %Manager{app: app, name: name, subscription: sub}=state) do
        Channel.acknowledge(app, name, number)
        %Manager{state | subscription: %Subscription{sub | ack: number}}
    end

    defp resume_process(%Manager{app: app, module: module}, id, ensure) do
        Saga.resume(app, {module, id}, ensure)
    end

    defp update_processes(%Manager{name: name, store: store, app: app}=state, procs) do
        args = [app, name, Enum.map(procs, &(Map.get(&1, :id)))]
        {:ok, _procs} = Kernel.apply(store, :set_state, args)
        %Manager{state | procs: procs}
    end

    defp push_event(%Event{number: number}=event, %Proc{}=proc) do
        %Proc{ saga: {pid, _ref} } =  proc
        Process.send(pid, event, [])
        %Proc{ proc | syn: number }
    end

    defp next_process_event(%Proc{syn: syn}, %Manager{app: app, topics: topics, store: store}) do
        store.list_events(app, topics, syn, 1)
    end

    defp start_process(%Manager{app: app, module: module}, id) do
        Saga.start(app, {module, id})
        |> GenServer.whereis()
    end

    defp init_process(%Manager{app: app, module: module}, id, number, ensure) do
        Saga.init(app, {module, id}, number, ensure)
    end

    defmacro __before_compile__(_env) do
        quote generated: true do
            def handle(_event), do: :ok
        end
    end
end
