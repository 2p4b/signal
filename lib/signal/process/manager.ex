defmodule Signal.Process.Manager do
    
    defmacro __using__(opts) do
        app = Keyword.get(opts, :application)
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        stop_timeout = Keyword.get(opts, :stop_timeout, 100)

        quote location: :keep do

            use GenServer
            alias Signal.Helper
            alias Signal.Events.Event
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
            def handle_info(:resume_processes, state) do
                Manager.handle_resume_processes(state)
            end


            @impl true
            def handle_info({:ack, id, number, ack}, state) do
                IO.inspect(number, label: "[man] #{inspect(ack)} ack")
                Manager.handle_ack({id, number, ack}, state)
            end

            @impl true
            def handle_info({:ack, id, number}, state) do
                IO.inspect(number, label: "[man] ack")
                Manager.handle_ack({id, number}, state)
            end

            @impl true
            def handle_info({:DOWN, ref, :process, _obj, _rsn}, manager) do
                Manager.handle_down(ref, manager)
            end

            @impl true
            def handle_info(%Event{}=event, state) do
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

    defstruct [:name, :store, :procs, :app, :topics, :subscription, :module]


    defmodule Proc do
        defstruct [:id, :saga, :ack, :syn, load: 0, status: :running]

        def new(id, pid, index \\ 0) do
            saga = {pid, Process.monitor(pid)}
            struct(__MODULE__, [id: id, saga: saga, ack: index, syn: index])
        end
    end

    alias Signal.Subscription
    alias Signal.Process.Saga
    alias Signal.Events.Event
    alias Signal.Process.Manager
    alias Signal.Channels.Channel



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
                {_index, payload} ->
                    {load_processes(state, payload), []}
                _ -> 
                    {[], []}
            end
        #Process.send(self(), :resume_processes, [])
        sub = Channel.subscribe(app, name, topics, opts)
        {:noreply, %Manager{state| procs: procs, subscription: sub}}
    end

    def handle_resume_processes(%Manager{procs: procs}=state) do
        procs = for proc <- procs, do: pull_event(state, proc)
        {:noreply, %Manager{state | procs: procs} }
    end

    def handle_down(ref, %Manager{procs: procs}=state) do

        pin = 
            Enum.find_index(procs, fn 
                %Proc{saga: {_pid, ^ref}} -> 
                    true
                _ ->
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else
            %Proc{id: id, status: status, ack: ack} = Enum.at(procs, pin)

            Process.demonitor(ref)

            pid = start_process(state, id) 

            {:ok, ack} = signal_resume(state, id, ack, true)

            proc = 
                Proc.new(id, pid, ack)
                |> Map.put(:load, 0)
                |> Map.put(:status, status)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, %Manager{state | procs: procs}}
        end
    end

    def handle_ack({id, number}, %Manager{procs: procs }=state) do
        pin = Enum.find_index(procs, &(Map.get(&1, :id) == id))

        if is_nil(pin) do
            {:noreply, state}
        else
            proc = Enum.at(procs, pin)

            load = if proc.load == 0, do: 0, else: proc.load - 1

            proc = %Proc{proc | ack: number, load: load}

            proc = pull_event(state, proc)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, update_processes(state, procs)}
        end
    end

    def handle_ack({id, number, :stop}, %Manager{procs: procs}=state) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, status: :halted} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else
            proc = Enum.at(procs, pin)

            proc = %Proc{proc | ack: number, status: :stopped}

            procs =  List.replace_at(procs, pin, proc)

            state = update_processes(state, procs)

            %Proc{ saga: {_pid, ref} } = proc

            Process.demonitor(ref)

            procs =  List.delete_at(procs, pin)

            {:noreply, update_processes(state, procs)}
        end
    end

    def handle_ack({id, _number, :continue}, %Manager{procs: procs}=state) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, status: :halted} -> 
                    true
                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else

            proc = Enum.at(procs, pin)

            proc = Map.put(proc, :status, :running)

            # Check if process has falled behind on event
            # and process has low load
            proc = pull_event(state, proc)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, update_processes(state, procs)}
        end
    end

    def handle_alive(id, %Manager{procs: procs}=state) do
        found =
            case Enum.find(procs, &(Map.get(&1, :id) == id))  do
                %Proc{status: :running} ->  true
                _ -> false
            end
        {:reply, found, state}
    end

    def handle_event(%Event{number: number}=event, %Manager{}=state) do

        %{module: module, subscription: %{ack: index}}= state

        procs =
            case Kernel.apply(module, :handle, [Event.payload(event)]) do

                {:halt, id}  ->
                    pin = Enum.find_index(state.procs, &(Map.get(&1, :id) == id))

                    if is_nil(pin) do
                        state.procs
                    else
                        proc = Enum.at(state.procs, pin)

                        proc = %Proc{ proc | status: {:halt, number} }

                        List.replace_at(state.procs, pin, proc)
                    end

                {start, id} when start in [:start, :start!] ->

                    pid = start_process(state, id)
                    proc = Proc.new(id, pid, index)
                        
                    case signal_init(proc, start) do
                        {:ok, _state} ->
                            state.procs ++ List.wrap(proc)
                        _ ->
                            state.procs
                    end

                {resume, id} when resume in [:resume, :resume!] ->

                    proc = Enum.find(state.procs, &(Map.get(&1, :id) == id))

                    if is_nil(proc) do
                        force = resume == :resume!
                        pid = start_process(state, id) 
                        {:ok, ack} = signal_resume(state, id, index, force)
                        proc = Proc.new(id, pid, ack)
                        state.procs ++  List.wrap(proc)
                    else
                        state.procs
                    end

                _unkown  ->
                    state.procs
            end
            |> Enum.map(fn 

                %Proc{syn: ^index, ack: ^index, load: 0, status: :running}=proc -> 
                    push_event(event, proc)

                %Proc{syn: ^index, ack: ^index, status: {:halt, ^number}}=proc -> 
                    if proc.load == 0, do: push_event(event, proc), else: proc

                proc -> 
                    proc 
            end)

        state = acknowledge(event, state)

        {:noreply, update_processes(state, procs)}
    end

    defp acknowledge(%Event{number: number}, %Manager{app: app, name: name}=state) do
        Channel.acknowledge(app, name, number)
        %Manager{subscription: sub} = state
        %Manager{state | subscription: %Subscription{sub | ack: number}}
    end

    defp update_processes(%Manager{name: name, store: store, app: app}=state, procs) do
        args = [app, name, dump_processes(procs)]
        {:ok, _procs} = Kernel.apply(store, :set_state, args)
        %Manager{state | procs: procs}
    end

    defp push_event(%Event{number: number}=event, %Proc{}=proc) do
        %Proc{ saga: {pid, _ref}, load: load, status: status } =  proc

        case status do

            {:halt, ^number} ->
                Process.send(pid, {:halt, event}, [])
                IO.inspect(number, label: "[man] expect stop ack")
                %Proc{ proc | syn: number, load: load + 1, status: :halted}

            {:halt, brk} when  brk <  number ->
                Process.send(pid, event, [])
                IO.inspect(number, label: "[man] expect ack")
                %Proc{ proc | syn: number, load: load + 1 }

            :running ->
                Process.send(pid, event, [])
                IO.inspect(number, label: "[man] expect ack")
                %Proc{ proc | syn: number, load: load + 1 }
        end
    end

    defp start_process(%Manager{app: app, module: module}, id) do
        Saga.start(app, {module, id})
        |> GenServer.whereis()
    end

    defp signal_resume(%Manager{app: app, module: module}, id, ack, ensure) do
        Saga.resume(app, {module, id}, ack, ensure)
    end

    defp signal_init(%Proc{ack: ack, saga: {pid, _ref}}, init) do
        GenServer.call(pid, {init, ack})
    end

    defp dump_processes(procs) when is_list(procs) do
        Enum.map(procs, fn %Proc{id: id, ack: ack, status: status} -> 
            [id, ack, status]
        end)
    end

    defp load_processes(state, payload) when is_list(payload) do
        Enum.map(payload, fn [id, ack, status] -> 
            pid = start_process(state, id) 
            {:ok, ack} = signal_resume(state, id, ack, true)
            Proc.new(id, pid, ack) 
            |> Map.put(:status, status)
        end)
    end

    defp pull_event(%Manager{}=state, %Proc{syn: syn, load: load, ack: ack}=proc) do
        %Manager{store: store, topics: topics, app: app, subscription: sub } = state

        if syn == ack and syn < sub.ack and load == 0 do
            args = [app, topics, syn, 1]
            events = Kernel.apply(store, :list_events, args)
            Enum.reduce(events, proc, fn event, proc -> 
                push_event(event, proc)                    
            end)
        else
            proc
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true do
            def handle(_event), do: :ok
        end
    end
end
