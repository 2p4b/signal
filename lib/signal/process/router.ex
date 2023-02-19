defmodule Signal.Process.Router do

    alias Signal.Event
    alias Signal.Event.Broker
    alias Signal.Process.Saga
    alias Signal.Process.Router
    alias Signal.Process.Supervisor

    defstruct [:name, :processes, :app, :topics, :consumer, :module, :timeout]

    defmodule Proc do
        alias Signal.Process.Router
        defstruct [:id, :pid, :ack, :syn, :ref, :type, :status, queue: []]

        def new(id, pid, opts \\ []) 
        def new(id, pid, opts) when is_map(opts) do
            new(id, pid, Map.to_list(opts))
        end

        def new(id, pid, opts) do
            {ref, pid} = 
                cond do
                    is_pid(pid) ->
                        {Process.monitor(pid), pid}

                    is_nil(pid) ->
                        {nil, nil}

                    true ->
                        {nil, nil}
                end

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
                    [
                        process: router.name,
                        saga: proc.id,
                        status: :running,
                        action: action,
                        push: number,
                    ]
                    |> Signal.Logger.info(label: :router)

                    GenServer.cast(pid, {action, event})
                    %Proc{proc | syn: number, queue: queue, status: :running}

                number > syn  ->
                    [
                        process: router.name,
                        saga: proc.id,
                        status: proc.status,
                        queued: number
                    ]
                    |> Signal.Logger.info(label: :router)
                    %Proc{proc | queue: queue}

                true ->
                    proc
            end
        end

        def acknowledge(%Proc{}=proc, number) do
            %Proc{ack: ack, syn: syn, queue: queue} =  proc
            queue = Enum.filter(queue, fn x -> x > number end)

            # Only acknowledge events with greater numbers
            # ie more recent events
            number = if number > ack, do: number, else: ack

            # On init ack syn === 0 
            # sync with ack from
            # process saga
            syn = if syn == 0 and number > syn, do: number, else: syn

            %Proc{proc | syn: syn, ack: number, queue: queue}
        end

    end

    def init(opts) do
        opts = 
            opts
            |> Keyword.put(:processes, [])
            |> Keyword.put_new_lazy(:timeout, fn -> Signal.Timer.seconds(30) end)
        {:ok, struct(__MODULE__, opts), {:continue, :boot}}
    end

    def handle_boot(%Router{}=router) do

        %Signal.Effect{data: data, number: number} = router_effect(router)

        router = 
            router
            |> load_processes(data)
            |> subscribe_router(number)
            |> track_router()
            
        {:noreply, router, router.timeout}
    end

    def handle_timeout(%Router{}=router) do
        {:noreply, router, :hibernate}
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
            {:noreply, router, router.timeout}
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

                    {:noreply, %Router{router | processes: processes}, router.timeout}

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

                    {:noreply, %Router{router|processes: processes}, router.timeout}
            end

        end
    end

    def handle_ack({:running, id, number}, %Router{}=router) do

        %Router{processes: processes}=router

        [
            process: router.name,
            saga: id,
            status: :running,
            ack: number,
        ]
        |> Signal.Logger.info(label: :router)

        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply, router, router.timeout}
        else

            process = 
                processes
                |> Enum.at(index)
                |> Proc.acknowledge(number)


            if not Enum.empty?(process.queue) do
                sched_next(process)
            end

            processes =  
                processes
                |> List.replace_at(index, process)

            {:noreply, save_state(router, processes), router.timeout}

        end
    end

    def handle_ack({:shutdown, id, number}, %Router{}=router) do
        %Router{processes: processes}=router
        [
            process: router.name,
            saga: id,
            status: :shutdown,
        ]
        |> Signal.Logger.info(label: :router)

        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true
                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply, router, router.timeout}
        else

            case Enum.at(processes, index) do
                %{pid: pid} when is_pid(pid) ->
                    process = Enum.at(processes, index)
                    stop_process(router, process)

                _ ->
                    nil
            end

            processes = 
                processes
                |> Enum.filter(fn %{id: sid} ->  sid != id end)

            {:noreply, save_state(router, processes), router.timeout}
        end
    end

    def handle_ack({:sleep, id, number}, %Router{}=router) do
        %Router{processes: processes}=router

        [
            process: router.name,
            saga: id,
            status: :sleeping,
        ]
        |> Signal.Logger.info(label: :router)

        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id, syn: ^number} -> 
                    true
                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply, router, router.timeout}
        else

            process = 
                processes
                |> Enum.at(index)
                |> Proc.acknowledge(number)

            # Demonitor if process queue is empty
            # else ignor stopped event
            processes =
                case process do
                    %{queue: [], syn: syn, ack: ack} when syn === ack ->
                        process = park_process(router, process)
                        List.replace_at(processes, index, process)

                    %{queue: _queue} ->
                        sched_next(process)
                        List.replace_at(processes, index, process)
                end

            {:noreply, %Router{router| processes: processes}, router.timeout}
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
        {:reply, found, router, router.timeout}
    end

    def handle_event(%Event{}=event, %Router{}=router) do
        %{module: module, processes: processes}= router

        reply = 
            case Kernel.apply(module, :handle, [Event.data(event)]) do
                {action, id} when (action in [:start, :apply]) and is_binary(id) ->
                    {action, id}

                :skip ->
                    {:skip, event.number}

                returned ->
                    raise """
                    process #{inspect(module)}.handle/1
                            expected return type of 
                            {:start, String.t()} | {:apply, String.t()} | skip
                            got #{inspect(returned)}
                    """
            end

        {action, id} = reply

        index = 
            processes
            |> Enum.find_index(&(Map.get(&1, :id) == id))

        [
            process: router.name,
            routing: event.topic,
            number: event.number,
            handle: reply,
        ]
        |> Signal.Logger.info(label: :router)
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

            {:noreply, save_state(router, processes), router.timeout}
        else
            router = acknowledge(router, event)
            {:noreply, save_state(router, processes), router.timeout}
        end
    end

    def handle_next(id, %Router{processes: processes}=router) do

        case Enum.find(processes, nil, &(Map.get(&1, :id) == id)) do
            nil ->
                nil

            %{queue: []} ->
                nil

            %{queue: [number|_]} ->
                event = 
                    router.app
                    |> Signal.Store.Adapter.get_event(number)
                Process.send(self(), event, [])
        end

        {:noreply, router, router.timeout}
    end

    defp acknowledge(%Router{}=router, %Event{}=event) do
        %Event{number: number} = event
        %Router{
            app: application, 
            consumer:  consumer
        } = router

        if number > consumer.ack  do
            [process: router.name, ack: number]
            |> Signal.Logger.info(label: :router)

            application
            |> Broker.acknowledge(consumer, number)
            %Router{router| consumer: Map.put(consumer, :ack, number)}
        else
            router
        end
    end

    defp save_state(%Router{}=router, processes) do

        %Router{
            app: application,
            name: name, 
            consumer: %{ack: ack}
        } = router

        data = dump_processes(processes)

        namespace = "Signal.Process"

        effect = 
            [id: name, namespace: namespace, data: data, number: ack]
            |> Signal.Effect.new()

        :ok = 
            application
            |> Signal.Store.Adapter.save_effect(effect)
        %Router{router| processes: processes}
    end

    defp sched_next(%Proc{id: id}) do
        Process.send(self(), {:next, id}, [])
    end

    defp start_process(router, id, index \\ 0)
    defp start_process(%Router{app: app, module: module}, id, index) do
        Saga.start(app, {id, module}, index)
        |> GenServer.whereis()
    end

    defp dump_processes(processes) when is_list(processes) do
        processes
        |> Enum.map(fn %Proc{id: id, ack: ack, queue: queue, status: status} -> 
            %{
                "id" => id, 
                "ack" => ack, 
                "queue" => Enum.uniq(queue),
                "status" => Atom.to_string(status)
            }
        end)
    end

    defp load_processes(%Router{module: module}=router, data) do
        processes = 
            data
            |> Enum.map(fn 
                %{"id" => id, "queue" => []} -> 
                    Proc.new(id, nil, queue: [], type: module)

                %{"id" => id, "queue" => queue} -> 
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

    defp park_process(%Router{}=router, %Proc{pid: pid, ref: ref}=process) 
    when is_pid(pid) do
        pname = Supervisor.process_name({process.id, process.type})
        Process.demonitor(ref)
        router.app
        |> Supervisor.stop_child(pname)
        %Proc{process | pid: nil, ref: nil, status: :sleeping}
    end

    defp stop_process(%Router{}=router, %Proc{pid: pid, ref: ref}=process) 
    when is_pid(pid) do
        pname = Supervisor.process_name({process.id, process.type})
        Process.demonitor(ref)
        router.app
        |> Supervisor.stop_child(pname)
        %Proc{process | pid: nil, ref: nil, status: :stopped}
    end

    defp subscribe_router(%Router{}=router, start) do
        %Router{app: application, name: name, topics: topics}=router

        subopts = [topics: topics, start: start]

        consumer = 
            application
            |> Broker.subscribe(name, subopts)

        %Router{router | consumer: consumer}
    end

    defp router_effect(%Router{}=router) do
        %Router{app: app, name: name}=router

        router_uuid = Signal.Effect.uuid("Signal.Process", name) 
        case Signal.Store.Adapter.get_effect(app, router_uuid) do
            %Signal.Effect{}=effect-> 
                effect

            _ ->
                %Signal.Effect{
                    id: name,
                    namespace: "Signal.Process",
                    number: Signal.Store.Adapter.get_cursor(app),
                    data: [],
                }
        end
    end

    defp track_router(%Router{}=router) do
        metadata = %{
            ts: DateTime.utc_now(),
            name: router.name,
            topics: router.topics,
            process: router.module,
            timeout: router.timeout,
            ack: router.consumer.ack,
        }
        router.app
        |> Signal.Tracker.track("router", router.name, metadata)
        router
    end

end
