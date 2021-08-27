defmodule Signal.Process.Router do

    alias Signal.Snapshot
    alias Signal.Process.Saga
    alias Signal.Stream.Event
    alias Signal.Process.Router

    defstruct [:name, :store, :procs, :app, :topics, :subscription, :module]


    defmodule Proc do
        defstruct [:id, :pid, :ack, :syn, :ref, queue: []]

        def new(id, pid, index \\ 0) do
            ref = Process.monitor(pid)
            opts = [
                id: id, 
                ref: ref, 
                pid: pid, 
                syn: index,
                ack: index, 
            ]
            struct(__MODULE__, opts)
        end

        def push_event(%Proc{}=proc, {action, %Event{number: number}=event}) do
            %Proc{ pid: pid, syn: syn, ack: ack, queue: queue} =  proc

            cond do
                syn == ack and number > ack  and queue == [] ->
                    GenServer.cast(pid, {action, event})
                    queue = Enum.filter(queue, fn 
                        {_action, ^number} -> false
                        _ -> true
                    end)
                    %Proc{ proc | syn: number, queue: queue }

                syn == ack and number > ack  ->
                    queue = queue ++ List.wrap({action, number})
                    %Proc{ proc | queue: queue }

                true ->
                    proc
            end
        end

    end

    def init(opts) do
        application = Keyword.get(opts, :application)
        app = {application, Keyword.get(opts, :app, application)}
        name = Keyword.get(opts, :name)
        topics = Keyword.get(opts, :topics)
        Process.send(self(), :boot, [])
        params = [
            app: app, 
            name: name, 
            procs: [],
            topics: topics,
            module: Keyword.get(opts, :module),
        ]
        {:ok, struct(__MODULE__, Keyword.merge(opts, params) )}
    end

    def handle_boot(%Router{app: app, name: name}=state) do
        {application, tenant} = app

        %Snapshot{data: data} = 
            case application.snapshot(name, tenant: tenant) do
                nil ->
                    %Snapshot{data: [], id: name, version: 0}

                snapshot -> 
                    snapshot
            end
        procs = Enum.map(data, fn {id, ack} -> 
            pid = start_process(state, id)                        
            signal_continue(state, id, ack, true)
            |> IO.inspect()
            Proc.new(id, pid, ack)
        end)

        %{ack: from} = 
            procs
            |> Enum.min_by(&(Map.get(&1, :ack)), fn ->  %{ack: 0} end)

        {:ok, sub} = application.subscribe(name, from: from, tenant: tenant)

        {:noreply, %Router{state | subscription: sub, procs: procs}}
    end

    def handle_down(%Router{procs: procs}=state, ref) do

        pin = 
            Enum.find_index(procs, fn 
                %Proc{ref: ^ref} -> 
                    true
                _ ->
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else

            proc = Enum.at(procs, pin)

            %Proc{id: id, ack: ack, syn: syn, queue: queue} = proc

            Process.demonitor(ref)

            pid = start_process(state, id) 

            {_ack, _status} = signal_continue(state, id, ack, true)

            queue =
                cond do
                    syn > ack and queue == [] ->
                        List.wrap({:apply, syn})
                        
                    true ->
                        queue
                end

            ref = Process.monitor(pid)

            proc = %Proc{proc | ref: ref, pid: pid, queue: queue} 

            sched_next(proc)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, %Router{state | procs: procs}}
        end
    end

    def handle_ack(%Router{procs: procs}=state, {id, number, :running}) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true
                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else

            proc = Enum.at(procs, pin)

            proc = struct(proc, %{ack: number})

            if not Enum.empty?(proc.queue) do
                sched_next(proc)
            end

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, log_state(state, procs)}
        end
    end

    def handle_ack(%Router{procs: procs}=state, {id, number, :stopped}) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply, state}
        else

            %Proc{ref: ref} = Enum.at(procs, pin)

            Process.demonitor(ref)

            procs =  List.delete_at(procs, pin)

            {:noreply, log_state(state, procs)}
        end
    end


    def handle_alive(%Router{procs: procs}=state, id) do
        found =
            case Enum.find(procs, &(Map.get(&1, :id) == id))  do
                %Proc{} ->  true
                _ -> false
            end
        {:reply, found, state}
    end

    def handle_event(%Router{}=state, %Event{}=event) do

        %{module: module, procs: procs, subscription: %{ack: ack}}= state

        {action, id} = Kernel.apply(module, :handle, [Event.payload(event)])

        index = Enum.find_index(procs, &(Map.get(&1, :id) == id))

        proc =
            case {action, id} do

                {:start, id}  ->

                    if is_nil(index) do
                        pid = start_process(state, id)
                        Proc.new(id, pid, ack)
                    else
                        Enum.at(procs, index)
                    end

                {:apply, _id} ->
                    Enum.at(procs, index)

                {:halt, _id}  ->
                    Enum.at(procs, index)

                        
                {:start!, id}  ->
                    if is_nil(index) do
                        pid = start_process(state, id)
                        Proc.new(id, pid, index)
                    else
                        Enum.at(procs, index)
                        |> IO.inspect(label: "PROCESS ALREADY UP")
                        nil
                    end


                _unknown  -> nil
            end

        if proc do
            proc = Proc.push_event(proc, {action, event})

            procs = 
                if is_nil(index) do
                    procs ++ [proc]
                else
                    List.update_at(procs, index, proc)
                end

            state = %Router{state | procs: procs}

            state = acknowledge(state, event)

            {:noreply, log_state(state, procs)}
        end
    end

    defp handle_next(%Router{app: app, procs: procs}=state, id) do

        {application, tenant} = app

        index = Enum.find_index(procs, &(Map.get(&1, :id) == id))

        router =
            if is_nil(index) do
                state
            else
                proc = Enum.at(procs, index)
                {action, number} = List.first(proc.queue)
                event = application.event(number, tenant: tenant)
                proc = Proc.push_event(proc, {action, event})
                procs = List.update_at(procs, index, proc)
                %Router{state | procs: procs}
            end
        {:noreply, router}
    end

    defp acknowledge(%Router{app: app}=state, %Event{number: number}) do
        {application, tenant} = app
        application.acknowledge(number, tenant: tenant)
        %Router{subscription: sub} = state
        %Router{state | subscription: Map.put(sub, :ack, number)}
    end

    defp log_state(%Router{}=state, procs) do
        %Router{state | procs: procs}
    end

    defp sched_next(%Proc{id: id}) do
        Process.send(self(), {:next, id}, [])
    end

    defp start_process(%Router{app: app, module: module}, id) do
        Saga.start(app, {module, id})
        |> GenServer.whereis()
    end

    defp signal_continue(%Router{app: app, module: module}, id, ack, ensure) do
        Saga.continue(app, {module, id}, ack, ensure)
    end

    defp signal_start(%Proc{ack: ack, pid: pid}, init) do
        GenServer.call(pid, {init, ack})
    end

    defp dump_processes(procs) when is_list(procs) do
        Enum.map(procs, fn %Proc{id: id, ack: ack} -> 
            {id, ack}
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

end
