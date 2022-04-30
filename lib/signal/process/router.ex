defmodule Signal.Process.Router do

    alias Signal.Snapshot
    alias Signal.Snapshot
    alias Signal.Process.Saga
    alias Signal.Stream.Event
    alias Signal.Process.Router

    require Logger

    defstruct [:name, :store, :procs, :app, :topics, :subscription, :module]


    defmodule Proc do
        defstruct [:id, :pid, :ack, :syn, :ref, :status, queue: []]

        def new(id, pid, opts \\ []) do
            ref = Process.monitor(pid)
            syn = Keyword.get(opts, :syn, 0)
            ack = Keyword.get(opts, :ack, 0)
            queue = Keyword.get(opts, :queue, [])
            status = Keyword.get(opts, :ack, :init)
            struct(__MODULE__, [
                id: id, 
                ref: ref, 
                pid: pid, 
                syn: syn,
                ack: ack, 
                queue: queue,
                status: status
            ])
        end

        def push_event(%Proc{}=proc, {action, %Event{number: number}=event}) do

            %Proc{ pid: pid, syn: syn, ack: ack, queue: queue, status: status} =  proc

            qnext = Enum.min(queue, &<=/2, fn -> number end)

            runx = :running

            cond do
                status == runx and syn == ack and number > ack and qnext == number ->
                    GenServer.cast(pid, {action, event})
                    queue = 
                        Enum.filter(queue, fn 
                            ^number -> false
                            _number -> true
                        end)
                    %Proc{ proc | syn: number, queue: queue }

                number > syn  ->
                    %Proc{ proc | 
                        queue: queue ++ List.wrap(number) 
                    }

                true ->
                    proc
            end
        end

        def acknowledge(%Proc{status: status, syn: syn, queue: queue}=proc, number) do

            {status, syn} = 
                if status == :init do
                    {:running, number}
                else
                    {status, syn}
                end

            queue = Enum.filter(queue, fn x -> x > number end)

            struct(proc, %{ack: number, queue: queue, syn: syn, status: status})
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

    def handle_boot(%Router{}=router) do

        snapshot = router_snapshot(router)

        router = 
            router
            |> load_processes(snapshot)
            |> subscribe_router(snapshot)

        {:noreply, router}
    end

    def handle_down(%Router{procs: procs}=router, ref) do

        pin = 
            Enum.find_index(procs, fn 
                %Proc{ref: ^ref} -> 
                    true
                _ ->
                    false
            end)

        if is_nil(pin) do
            {:noreply,router}
        else

            proc = Enum.at(procs, pin)

            %Proc{id: id, ack: ack, syn: syn, queue: queue} = proc

            Process.demonitor(ref)

            pid = start_process(router, id, ack) 

            queue =
                cond do
                    syn > ack ->
                        List.wrap(syn) ++ queue
                        
                    true ->
                        queue
                end

            ref = Process.monitor(pid)

            proc = %Proc{proc | ref: ref, pid: pid, queue: queue} 

            sched_next(proc)

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, %Router{router| procs: procs}}
        end
    end

    def handle_ack(%Router{procs: procs}=router, {id, number, :running}) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, status: :init} -> 
                    true

                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply,router}
        else

            proc = 
                procs
                |> Enum.at(pin)
                |> Proc.acknowledge(number)


            if not Enum.empty?(proc.queue) do
                sched_next(proc)
            end

            procs =  List.replace_at(procs, pin, proc)

            {:noreply, log_state(router, procs)}
        end
    end

    def handle_ack(%Router{procs: procs}=router, {id, number, :stopped}) do
        pin =
            Enum.find_index(procs, fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(pin) do
            {:noreply,router}
        else

            %Proc{ref: ref} = Enum.at(procs, pin)

            Process.demonitor(ref)

            procs =  List.delete_at(procs, pin)

            {:noreply, log_state(router, procs)}
        end
    end


    def handle_alive(%Router{procs: procs}=router, id) do
        found =
            case Enum.find(procs, &(Map.get(&1, :id) == id))  do
                %Proc{} ->  true
                _ -> false
            end
        {:reply, found,router}
    end

    def handle_event(%Router{}=router, %Event{}=event) do

        %{module: module, procs: procs}= router

        {action, id} = Kernel.apply(module, :handle, [Event.payload(event)])

        index = Enum.find_index(procs, &(Map.get(&1, :id) == id))

        proc =
            case {action, index} do

                {:stop, index} ->
                    if is_nil(index) do
                        nil
                    else
                        Enum.at(procs, index)
                    end


                {:start, index}  ->
                    if is_nil(index) do
                        pid = start_process( router, id)
                        Proc.new(id, pid)
                    else
                        Enum.at(procs, index)
                    end

                {:start!, index}  ->
                    if is_nil(index) do
                        pid = start_process(router, id)
                        Proc.new(id, pid)
                    else
                        Enum.at(procs, index)
                        |> IO.inspect(label: "PROCESS ALREADY STARTED")
                        nil
                    end


                {:apply, index} when is_integer(index) ->
                    Enum.at(procs, index)


                {:halt, index}  when is_integer(index) ->
                    Enum.at(procs, index)

                        
                    _ ->  nil

            end

        if proc do
            proc = Proc.push_event(proc, {action, event})

            %Proc{queue: queue, ref: ref} = proc

            procs = 
                case {action, index} do
                    {:stop, index} when is_integer(index) and queue == [] ->
                        Process.demonitor(ref)
                        Enum.filter(procs, fn %Proc{id: pid} -> pid !== id end)

                    {_action, nil} ->
                        procs ++ [proc]

                    {_action, index} when is_integer(index) ->
                        fn_update_proc = fn _process -> proc end
                        List.update_at(procs, index, fn_update_proc)

                    _ ->
                        procs
                end

            router = %Router{router | procs: procs}

            router = acknowledge(router, event)

            {:noreply, log_state(router, procs)}
        else
            router = acknowledge(router, event)
            {:noreply, log_state(router, procs)}
        end
    end

    def handle_next(%Router{app: app, procs: procs}=router, id) do

        {application, tenant} = app

        index = Enum.find_index(procs, &(Map.get(&1, :id) == id))

        router =
            if is_nil(index) do
                router
            else
                proc = Enum.at(procs, index)
                number = List.first(proc.queue)
                event = application.event(number, tenant: tenant)
                Process.send(self(), event, [])
                router
            end
        {:noreply, router}
    end

    defp acknowledge(%Router{}=router, %Event{}=event) do
        %Event{number: number} = event
        %Router{
            app: {application, tenant}, 
            subscription: sub 
        } = router

        if number > sub.ack  do
            application.acknowledge(sub.handle, number, tenant: tenant)
            log(router, "acknowledged: #{number}")
            %Router{router| subscription: Map.put(sub, :ack, number)}
        else
            router
        end
    end

    defp log_state(%Router{}=router, procs) do

        %Router{
            app: {application, tenant},
            name: name, 
            subscription: %{ack: ack}
        } = router

        processes = dump_processes(procs)

        name
        |> Snapshot.new(processes, [version: ack])
        |> application.record([tenant: tenant])

        %Router{router| procs: procs}
    end

    defp sched_next(%Proc{id: id}) do
        Process.send(self(), {:next, id}, [])
    end

    defp start_process(router, id, index \\ 0)
    defp start_process(%Router{app: app, module: module}, id, index) do
        Saga.start(app, {module, id}, index)
        |> GenServer.whereis()
    end

    defp dump_processes(procs) when is_list(procs) do
        Enum.map(procs, fn %Proc{id: id, queue: queue} -> 
            %{id: id, queue: queue}
        end)
    end

    defp load_processes(%Router{}=router, %Snapshot{data: data}) do
        procs = 
            data
            |> Enum.filter(fn 
                %{queue: []} -> 
                    false
                _ ->
                    true
            end)
            |> Enum.map(fn %{id: id, queue: queue} -> 
                pid = start_process(router, id, 0)
                Proc.new(id, pid, queue: queue)
            end)

        %Router{router | procs: procs}
    end

    defp subscribe_router(%Router{}=router, %Snapshot{version: version}) do
        %Router{app: {application, tenant}, name: name, topics: topics}=router

        subopts = [topics: topics, from: version, tenant: tenant]

        {:ok, sub} = application.subscribe(name, subopts)

        %Router{router | subscription: sub}
    end

    defp router_snapshot(%Router{}=router) do
        %Router{app: {application, tenant}, name: name}=router

        case application.snapshot(name, tenant: tenant) do
            nil ->
                %Snapshot{data: [], id: name, version: 0}

            snapshot -> 
                snapshot
        end
    end

    def log(%Router{module: module}, info) do
        info = """ 

        [ROUTER] #{inspect(module)}
                 #{info}
        """
        Logger.info(info)
    end

end
