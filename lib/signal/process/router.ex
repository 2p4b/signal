defmodule Signal.Process.Router do

    @router_namespace "$router"

    alias Signal.Event
    alias Signal.Event.Broker
    alias Signal.Process.Saga
    alias Signal.Process.Router
    alias Signal.Process.Supervisor

    defstruct [:name, :processes, :app, :topics, :consumer, :module, :timeout, :uuid]

    defmodule Proc do
        alias Signal.Process.Router
        alias Signal.Process.Supervisor
        defstruct [
            :id, 
            :app,
            :uuid, 
            :pid, 
            :ack, 
            :syn, 
            :ref, 
            :namespace, 
            status: :running, 
            queue: []
        ]

        def new(opts) do
            id = Keyword.fetch!(opts, :id)
            pid = Keyword.get(opts, :pid)
            ack = Keyword.get(opts, :ack, 0)
            namespace = Keyword.fetch!(opts, :namespace)
            uuid = Signal.Effect.uuid(namespace, id)
            {ref, pid} = 
                cond do
                    is_pid(pid) ->
                        {Process.monitor(pid), pid}

                    is_nil(pid) ->
                        {nil, nil}

                    true ->
                        {nil, nil}
                end


            opts = 
                opts
                |> Keyword.merge([syn: ack, ack: ack, uuid: uuid, ref: ref, pid: pid])

            struct(__MODULE__, opts) 
        end

        def push_event(%Proc{}=proc, %Event{number: number}=event) do

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
                        process: proc.namespace,
                        saga: proc.id,
                        status: :running,
                        push: number,
                    ]
                    |> Signal.Logger.info(label: :router)

                    proc.app
                    |> Signal.PubSub.broadcast(proc.uuid, event)
                    %Proc{proc | syn: number, queue: queue, status: :running}

                number > syn  ->
                    [
                        process: proc.namespace,
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

        def stop(%Proc{app: app, ref: ref, uuid: uuid}=proc) do
            Process.demonitor(ref)
            Supervisor.unregister_child(app, uuid)
            Signal.PubSub.broadcast(app, uuid, :stopped)
            %Proc{proc | pid: nil, ref: nil, status: :stopped}
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
        name = Keyword.fetch!(opts, :name)
        uuid = Signal.Effect.uuid(@router_namespace, name)
        opts = 
            opts
            |> Keyword.put(:uuid, uuid)
            |> Keyword.put(:processes, [])
            |> Keyword.put_new_lazy(:timeout, fn -> Signal.Timer.seconds(30) end)
        {:ok, struct(__MODULE__, opts), {:continue, :boot}}
    end

    def handle_boot(%Router{}=router) do

        {data, number} = load_router_data(router)

        processes = Enum.map(data, fn proc -> create_proc(router, proc) end)

        router = 
            router
            |> struct(%{processes: processes})
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

                    process =
                        process
                        |> Map.from_struct()
                        |> Map.put(:pid, pid)
                        |> Map.to_list()
                        |> Proc.new()

                    sched_next(process)

                    processes =  
                        processes
                        |> List.replace_at(index, process)

                    {:noreply, %Router{router|processes: processes}, router.timeout}
            end

        end
    end

    def handle_ack({:start, id, number}, %Router{}=router) do
        %Router{processes: processes}=router

        [
            process: router.name,
            saga: id,
            status: :started,
            start: number,
        ]
        |> Signal.Logger.info(label: :router)

        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id} -> 
                    true
                _ -> 
                    false
            end)

        if is_nil(index) do
            {:noreply, router, router.timeout}
        else

            process = Enum.at(processes, index)

            if not Enum.empty?(process.queue) do
                sched_next(process)
            end
            {:noreply, router, router.timeout}
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

            router = 
                router
                |> struct(%{processes: processes})
                |> save_state()

            {:noreply, router, router.timeout}

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

    def handle_stop(id, %Router{}=router) do
        %Router{processes: processes}=router
        [
            process: router.name,
            saga: id,
            status: :stopped,
        ]
        |> Signal.Logger.info(label: :router)

        index =
            processes
            |> Enum.find_index(fn 
                %Proc{id: ^id} -> true
                _ -> false
            end)

        if is_nil(index) do
            {:noreply, router, router.timeout}
        else

            process = 
                processes
                |> Enum.at(index)

            case process do
                %{queue: [], pid: pid} when is_pid(pid) ->
                    processes
                    |> Enum.at(index)
                    |> Proc.stop()

                _ ->
                    nil
            end

            processes = 
                processes
                |> Enum.filter(fn %{id: sid} ->  sid != id end)

            router = 
                router
                |> struct(%{processes: processes})
                |> save_state()

            {:noreply, router, router.timeout}
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
        %{module: module, app: app, processes: processes, name: namespace}= router

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
                            {:start, String.t()} | {:apply, String.t()} | :skip
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
                        opts = [
                            id: id, 
                            app: app, 
                            pid: pid, 
                            namespace: namespace
                        ]
                        Proc.new(opts)
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
                |> Proc.push_event(event)

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

            router = 
                router
                |> acknowledge_event(event)
                |> struct(%{processes: processes})
                |> save_state()

            {:noreply, router, router.timeout}
        else
            router = 
                router
                |> acknowledge_event(event)
                |> struct(%{processes: processes})
                |> save_state()

            {:noreply, router, router.timeout}
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

    defp acknowledge_event(%Router{}=router, %Event{}=event) do
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

    defp save_state(%Router{}=router) do
        %Router{
            app: application,
            name: name, 
            uuid: effect_uuid,
            consumer: %{ack: ack}
        } = router

        dumped_processes = 
            router.processes
            |> Enum.map(fn process -> 
                dump_process(process) 
            end)

        data = %{
            "id" => name,
            "ack" => ack,
            "processes" => dumped_processes
        }

        effect = 
            [uuid: effect_uuid, namespace: @router_namespace, data: data]
            |> Signal.Effect.new()

        :ok = Signal.Store.Adapter.save_effect(application, effect)
        router
    end

    defp sched_next(%Proc{id: id}) do
        Process.send(self(), {:next, id}, [])
    end

    defp start_process(router, id, index \\ 0)
    defp start_process(%Router{}=router, id, index) do
        suuid = Signal.Effect.uuid(router.name, id)
        opts = [
            id: id, 
            app: router.app, 
            uuid: suuid, 
            start: index,
            module: router.module, 
            channel: router.uuid,
            namespace: router.name, 
        ]
        router.app
        |> Saga.start({suuid, router.module}, opts)
        |> GenServer.whereis()
    end

    defp dump_process(%Proc{}=process) do
        %Proc{id: id, ack: ack, queue: queue} = process
        %{
            "id" => id, 
            "ack" => ack, 
            "queue" => Enum.uniq(queue),
        }
    end

    defp create_proc(%Router{name: name, app: app}=router, data) when is_map(data) do
        common = [app: app, namespace: name, module: router.module]
        case data do
            %{"id" => id, "queue" => []} -> 
                common
                |> Keyword.merge([id: id, queue: []])
                |> Proc.new()

            %{"id" => id, "queue" => queue} -> 
                pid = start_process(router, id, 0)
                common
                |> Keyword.merge([id: id, queue: queue, pid: pid])
                |> Proc.new()
        end
    end

    defp wake_process(%Router{}=router, %Proc{pid: nil, status: :sleeping}=proc) do
        pid = start_process(router, proc.id, proc.ack)
        proc
        |> Map.from_struct() 
        |> Map.put(:pid, pid)
        |> Map.put(:status, :running)
        |> Map.to_list()
        |> Proc.new()
    end

    defp wake_process(%Router{}, %Proc{}=proc) do
        proc
    end

    defp park_process(%Router{}=router, %Proc{uuid: uuid, pid: pid, ref: ref}=process) 
    when is_pid(pid) do
        Process.demonitor(ref)
        router.app
        |> Supervisor.stop_child(uuid)
        %Proc{process | pid: nil, ref: nil, status: :sleeping}
    end

    defp subscribe_router(%Router{}=router, start) do
        %Router{
            app: application, 
            name: name, 
            uuid: uuid,
            topics: topics, 
            processes: processes,
        }=router

        start = 
            processes
            |> Enum.reduce([start], &(&1.queue ++ &2))
            |> Enum.max()

        subopts = [topics: topics, start: start, track: false]

        consumer = 
            application
            |> Broker.subscribe(name, subopts)

        application
        |> Signal.PubSub.subscribe(uuid)

        %Router{router | consumer: consumer}
    end

    defp load_router_data(%Router{}=router) do
        %Router{app: app, name: name}=router
        router_uuid = Signal.Effect.uuid(@router_namespace, name) 
        case Signal.Store.Adapter.get_effect(app, router_uuid) do
            %Signal.Effect{data: %{"ack" => ack, "processes" => processes}}-> 
                {processes, ack}

            _ ->
                {[], Signal.Store.Adapter.get_cursor(app)}
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
