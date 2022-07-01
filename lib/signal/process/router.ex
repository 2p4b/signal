defmodule Signal.Process.Router do

    alias Signal.Snapshot
    alias Signal.Snapshot
    alias Signal.Process.Saga
    alias Signal.Stream.Event
    alias Signal.Process.Router
    alias Signal.Process.Supervisor

    require Logger

    defstruct [:name, :processes, :app, :topics, :subscription, :module]


    defmodule Proc do
        alias Signal.Process.Router
        defstruct [:id, :pid, :ack, :syn, :ref, :type, :status, queue: []]

        def new(id, pid, opts \\ []) 
        def new(id, pid, opts) when is_nil(pid) do
            syn = Keyword.get(opts, :syn, 0)
            ack = Keyword.get(opts, :ack, 0)
            type = Keyword.get(opts, :type)
            queue = Keyword.get(opts, :queue, [])
            status = Keyword.get(opts, :ack, :sleeping)
            struct(__MODULE__, [
                id: id, 
                ref: nil, 
                pid: nil, 
                syn: syn,
                ack: ack, 
                type: type,
                queue: queue,
                status: status
            ])
        end

        def new(id, pid, opts) when is_pid(pid) and is_map(opts) do
            new(id, pid, Map.to_list(opts))
        end

        def new(id, pid, opts) when is_pid(pid) and is_list(opts) do
            ref = Process.monitor(pid)
            syn = Keyword.get(opts, :syn, 0)
            ack = Keyword.get(opts, :ack, 0)
            type = Keyword.get(opts, :type)
            queue = Keyword.get(opts, :queue, [])
            status = Keyword.get(opts, :status, :sleeping)
            struct(__MODULE__, [
                id: id, 
                ref: ref, 
                pid: pid, 
                syn: syn,
                ack: ack, 
                type: type,
                queue: queue,
                status: status
            ])
        end

        def push_event(%Proc{}=proc, {action, %Event{number: number}=event}, router) do

            %Proc{pid: pid, syn: syn, ack: ack, queue: queue} =  proc

            qnext = Enum.min(queue, &<=/2, fn -> number end)

            queue = 
                if Enum.member?(queue, number) do
                    queue 
                else
                    (queue ++ List.wrap(number))|> Enum.uniq()
                end

            cond do
                is_pid(pid) and syn == ack and number > ack and qnext == number ->
                    Router.log(router, """ 
                        process: #{proc.id}
                        status: :running syn: #{number}
                        """ )
                    GenServer.cast(pid, {action, event})
                    %Proc{ proc | syn: number, queue: queue, status: :running }

                number > syn  ->
                    Router.log(router, """ 
                        process: #{proc.id}
                        status: #{proc.status} queued: #{number}
                        """ )
                    %Proc{proc | queue: queue}

                true ->
                    proc
            end
        end

        def acknowledge(%Proc{}=proc, {status, number}) do
            %Proc{ack: ack, syn: syn, queue: queue} =  proc
            queue = Enum.filter(queue, fn x -> x > number end)

            # Only acknowledge events with greater numbers
            # ie more recent events
            number = if number > ack, do: number, else: ack

            # On init ack syn === 0 
            # sync with ack from
            # process saga
            syn = if syn == 0 and number > syn, do: number, else: syn

            %Proc{proc | syn: syn, ack: number, queue: queue, status: status}
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
            processes: [],
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

    def handle_down(ref, %Router{processes: processes}=router) do

        index = 
            processes
            |> Enum.find_index(fn 
                %Proc{ref: ^ref} -> 
                    true
                _ ->
                    false
            end)

        if is_nil(index) do
            {:noreply, router}
        else

            process = Enum.at(processes, index)


            case process.queue do
                [] ->

                    process =
                        %Proc{process | 
                            pid: nil, 
                            ref: nil, 
                            status: :sleeping
                        }

                    processes =  
                        processes
                        |> List.replace_at(index, process)

                    {:noreply, %Router{router | processes: processes}}

                _ ->

                    process.ref
                    |> Process.demonitor()

                    %Proc{id: id, ack: ack} = process

                    pid = start_process(router, id, ack) 

                    process = Proc.new(id, pid, Map.from_struct(process))

                    sched_next(process)

                    processes =  
                        processes
                        |> List.replace_at(index, process)

                    {:noreply, %Router{router|processes: processes}}
            end

        end
    end

    def handle_ack({:running, id, number}, %Router{}=router) do

        %Router{processes: processes}=router

        log(router, """
            process: #{id}
            status: running ack: #{number}
            """ )
        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id, status: :sleeping} -> 
                    true

                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply, router}
        else

            process = 
                processes
                |> Enum.at(index)
                |> Proc.acknowledge({:running, number})


            if not Enum.empty?(process.queue) do
                sched_next(process)
            end

            processes =  
                processes
                |> List.replace_at(index, process)

            {:noreply, log_state(router, processes)}
        end
    end

    def handle_ack({:sleeping, id, number}, %Router{}=router) do
        %Router{processes: processes}=router

        log(router, """
            process: #{id}
            status: sleeping ack: #{number}
            """ )
        index =
            Enum.find_index(processes, fn 
                %Proc{id: ^id, status: :sleeping} -> 
                    true

                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply, router}
        else

            process = 
                processes
                |> Enum.at(index)
                |> Proc.acknowledge({:sleeping, number})

            # Demonitor if process queue is empty
            # else ignor stopped event
            processes =
                case process do
                    %{queue: []} ->
                        process =
                            process
                            |> stop_process(router)
                            |> struct(%{ref: nil, pid: nil})
                        List.replace_at(processes, index, process)

                    %{queue: _queue} ->
                        sched_next(process)
                        List.replace_at(processes, index, process)
                end

            {:noreply, log_state(router, processes)}
        end
    end

    def handle_ack({:shutdown, id, number}, %Router{}=router) do
        %Router{processes: processes}=router
        log(router, """
            process: #{id}
            status: shutdown ack: #{number}
            """ )
        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply,router}
        else

            case Enum.at(processes, index) do
                %{pid: pid} when is_pid(pid) ->
                    Enum.at(processes, index)
                    |> stop_process(router)

                _ ->
                    nil
            end

            processes = 
                processes
                |> Enum.filter(fn %{id: sid} ->  sid != id end)
            {:noreply, log_state(router, processes)}
        end
    end



    def handle_alive(id, %Router{processes: processes}=router) do
        found =
            case Enum.find(processes, &(Map.get(&1, :id) == id))  do
                %Proc{} ->  
                    true
                _ -> 
                    false
            end
        {:reply, found, router}
    end

    def handle_event(%Event{}=event, %Router{}=router) do

        %{module: module, processes: processes}= router

        reply = Kernel.apply(module, :handle, [Event.payload(event)])

        {action, id} = reply

        index = 
            processes
            |> Enum.find_index(&(Map.get(&1, :id) == id))

        log(router, """
            routing: #{event.type}
            number: #{event.number}
            handle: #{inspect(reply)}
            """ )
        proc =
            case {action, index} do

                {:start, index}  ->
                    if is_nil(index) do
                        pid = start_process(router, id)
                        Proc.new(id, pid, type: module)
                    else
                        Enum.at(processes, index)
                    end

                {:apply, index} when is_integer(index) ->
                    Enum.at(processes, index, type: module)

                _ ->  
                    nil
            end

        if proc do

            log(router, """
                routing: #{event.type}
                number: #{event.number}
                process: #{proc.id} pid: #{inspect(proc.pid)}
                """ )
            process = 
                router
                |> wake_process(proc)
                |> Proc.push_event({action, event}, router)

            processes = 
                case {action, index} do
                    {_action, nil} ->
                        processes ++ [process]

                    {_action, index} when is_integer(index) ->
                        fn_update_proc = fn _process -> process end
                        processes
                        |> List.update_at(index, fn_update_proc)
                end

            router = %Router{router |processes: processes}

            router = acknowledge(router, event)

            {:noreply, log_state(router, processes)}
        else
            router = acknowledge(router, event)
            {:noreply, log_state(router, processes)}
        end
    end

    def handle_next(id, %Router{processes: processes}=router) do

        case Enum.find(processes, nil, &(Map.get(&1, :id) == id)) do
            nil ->
                nil

            %{queue: []} ->
                nil

            %{queue: [number|_]} ->
                {application, tenant} = router.app
                event = application.event(number, tenant: tenant)
                Process.send(self(), event, [])
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

    defp log_state(%Router{}=router, processes) do

        %Router{
            app: {application, tenant},
            name: name, 
            subscription: %{ack: ack}
        } = router

        data = dump_processes(processes)

        name
        |> Snapshot.new(data, [version: ack])
        |> application.record([tenant: tenant])

        %Router{router| processes: processes}
    end

    defp sched_next(%Proc{id: id}) do
        Process.send(self(), {:next, id}, [])
    end

    defp start_process(router, id, index \\ 0)
    defp start_process(%Router{app: app, module: module}, id, index) do
        Saga.start(app, {module, id}, index)
        |> GenServer.whereis()
    end

    defp dump_processes(processes) when is_list(processes) do
        processes
        |> Enum.map(fn %Proc{id: id, ack: ack, queue: queue, status: status} -> 
            %{
                id: id, 
                ack: ack, 
                queue: Enum.uniq(queue),
                status: Atom.to_string(status)
            }
        end)
    end

    defp load_processes(%Router{module: module}=router, %Snapshot{data: data}) do
        processes = 
            data
            |> Enum.map(fn 
                %{id: id, queue: []} -> 
                    Proc.new(id, nil, queue: [], type: module)

                %{id: id, queue: queue} -> 
                    pid = start_process(router, id, 0)
                    Proc.new(id, pid, queue: queue, type: module)
            end)

        %Router{router |processes: processes}
    end

    defp wake_process(%Router{}=router, %Proc{pid: nil, status: :sleeping}=proc) do
        pid = start_process(router, proc.id, 0)
        opts =
            proc
            |> Map.from_struct() 
            |> Map.put(:status, :running)

        Proc.new(proc.id, pid,  opts)
    end

    defp wake_process(%Router{}, %Proc{}=proc) do
        proc
    end

    defp stop_process(%Proc{pid: pid, ref: ref}=process, %Router{}=router) 
    when is_pid(pid) do
        pname = Supervisor.process_name({process.type, process.id})
        router.app
        |> Supervisor.stop_child(pname)
        Process.demonitor(ref)
        process
    end

    defp subscribe_router(%Router{}=router, %Snapshot{version: version}) do
        %Router{app: {application, tenant}, name: name, topics: topics}=router

        subopts = [topics: topics, start: version, tenant: tenant]

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
        [ROUTER]: #{inspect(module)}
        #{info}
        """
        Logger.info(info)
    end

end
