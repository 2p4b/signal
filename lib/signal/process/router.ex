defmodule Signal.Process.Router do

    defstruct [:name, :store, :procs, :app, :topics, :subscription, :module]


    defmodule Proc do
        defstruct [:id, :saga, :ack, :syn, load: 0, advisory: [], status: :running]

        def new(id, pid, index \\ 0) do
            saga = {pid, Process.monitor(pid)}
            struct(__MODULE__, [id: id, saga: saga, ack: index, syn: index])
        end
    end

    alias Signal.Subscription
    alias Signal.Process.Saga
    alias Signal.Stream.Event
    alias Signal.Process.Router
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

    def handle_init(%Router{}=state) do
        %Router{store: store, name: name, app: app, topics: topics} = state
        {procs, opts} = 
            case store.get_state(app, name, :max) do
                {_index, payload} ->
                    {load_processes(state, payload), []}
                _ -> 
                    {[], []}
            end
        Process.send(self(), :start_processes, [])
        sub = Channel.subscribe(app, name, topics, opts)
        {:noreply, %Router{state| procs: procs, subscription: sub}}
    end

    def handle_start_processes(%Router{procs: procs}=state) do
        procs = for proc <- procs, do: pull_event(state, proc)
        {:noreply, %Router{state | procs: procs} }
    end

    def handle_down(ref, %Router{procs: procs}=state) do

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
            %Proc{id: id, ack: ack} = Enum.at(procs, pin)

            Process.demonitor(ref)

            pid = start_process(state, id) 

            {ack, status} = signal_continue(state, id, ack, true)

            proc = 
                Proc.new(id, pid, ack)
                |> Map.put(:load, 0)
                |> Map.put(:status, status)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, %Router{state | procs: procs}}
        end
    end

    def handle_ack({id, number, :running}, %Router{procs: procs}=state) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, status: :halted} -> 
                    true

                %Proc{id: ^id, status: {:halt, hnum} } when hnum > number -> 
                    true

                %Proc{id: ^id, status: :running} -> 
                    true

                %Proc{id: ^id, status: :stopped} -> 
                    true
                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else

            proc = Enum.at(procs, pin)

            load = if proc.load == 0, do: 0, else: proc.load - 1

            status = 
                case proc.status do
                    {:halt, _} ->
                        proc.status

                    _ ->
                        :running
                end


            proc = struct(proc, %{ack: number, status: status, load: load})

            # Check if process has falled behind on event
            # and process has low load
            proc = pull_event(state, proc)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, log_state(state, procs)}
        end
    end

    def handle_ack({id, _number, :stopped}, %Router{procs: procs}=state) do
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

            %Proc{ saga: {_pid, ref} } = Enum.at(procs, pin)

            Process.demonitor(ref)

            procs =  List.delete_at(procs, pin)

            {:noreply, log_state(state, procs)}
        end
    end


    def handle_alive(id, %Router{procs: procs}=state) do
        found =
            case Enum.find(procs, &(Map.get(&1, :id) == id))  do
                %Proc{status: :running} ->  true
                _ -> false
            end
        {:reply, found, state}
    end

    def handle_event(%Event{number: number}=event, %Router{}=state) do

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
                        
                    case signal_start(proc, start) do
                        {ack, status} ->
                            proc = struct(proc, %{ack: ack, status: status}) 
                            state.procs ++ List.wrap(proc)
                        _ ->
                            state.procs
                    end

                {continue, id} when continue in [:continue, :continue!] ->

                    proc = Enum.find(state.procs, &(Map.get(&1, :id) == id))

                    if is_nil(proc) do
                        force = continue == :continue!
                        pid = start_process(state, id) 
                        {ack, status} = signal_continue(state, id, index, force)
                        proc = 
                            Proc.new(id, pid, ack)
                            |> struct(%{status: status, ack: ack})

                        state.procs ++  List.wrap(proc)
                    else
                        state.procs
                    end

                _unknown  ->
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

        {:noreply, log_state(state, procs)}
    end

    defp acknowledge(%Event{number: number}, %Router{app: app, name: name}=state) do
        Channel.acknowledge(app, name, number)
        %Router{subscription: sub} = state
        %Router{state | subscription: %Subscription{sub | ack: number}}
    end

    defp log_state(%Router{name: name, store: store, app: app}=state, procs) do
        args = [app, name, dump_processes(procs)]
        {:ok, _procs} = Kernel.apply(store, :set_state, args)
        %Router{state | procs: procs}
    end

    defp push_event(%Event{number: number}=event, %Proc{}=proc) do
        %Proc{ saga: {pid, _ref}, load: load, status: status } =  proc

        case status do

            {:halt, ^number} ->
                Process.send(pid, {:halt, event}, [])
                %Proc{ proc | syn: number, load: load + 1, status: :halted}

            {:halt, brk} when  brk <  number ->
                Process.send(pid, event, [])
                %Proc{ proc | syn: number, load: load + 1 }

            :running ->
                Process.send(pid, event, [])
                %Proc{ proc | syn: number, load: load + 1 }
        end
    end

    defp start_process(%Router{app: app, module: module}, id) do
        Saga.start(app, {module, id})
        |> GenServer.whereis()
    end

    defp signal_continue(%Router{app: app, module: module}, id, ack, ensure) do
        Saga.continue(app, {module, id}, ack, ensure)
    end

    defp signal_start(%Proc{ack: ack, saga: {pid, _ref}}, init) do
        GenServer.call(pid, {init, ack})
    end

    defp dump_processes(procs) when is_list(procs) do
        Enum.map(procs, fn %Proc{id: id, ack: ack, status: status} -> 
            [id, ack, status]
        end)
    end

    defp load_processes(state, payload) when is_list(payload) do
        Enum.map(payload, fn [id, ack, _status] -> 
            pid = start_process(state, id) 
            {ack, status} = signal_continue(state, id, ack, true)
            Proc.new(id, pid, ack) 
            |> Map.put(:status, status)
        end)
    end

    defp pull_event(%Router{}=state, %Proc{syn: syn, load: load, ack: ack}=proc) do
        %Router{store: store, topics: topics, app: app, subscription: sub } = state

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

end
