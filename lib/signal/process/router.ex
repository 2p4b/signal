defmodule Signal.Process.Router do

    @router_namespace "$router"

    alias Signal.Event
    alias Signal.Effect
    alias Signal.Event.Broker
    alias Signal.Process.Saga
    alias Signal.Process.Router
    alias Signal.Process.Supervisor

    defstruct [
        :app, 
        :uuid,
        :name, 
        :topics, 
        :module, 
        :timeout, 
        :consumer, 
        :instances,
        :processing,
    ]

    defmodule Instance do
        alias Signal.Effect
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
            uuid = Effect.uuid(namespace, id)
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

        def push_event(%Instance{}=instance, %Event{number: number}=event) do

            %Instance{pid: pid, syn: syn, ack: ack, queue: queue} = instance

            next = List.first(queue, event)

            queue = 
                cond do
                    Enum.empty?(queue) ->
                        # add event to empty queue
                        List.wrap(event)

                    next.number === event.number
                        # old event at head do nothing
                            queue
                    true ->
                        # put new event in queue
                        queue
                        |> Enum.concat(List.wrap(event))
                        |> Enum.uniq_by(&(Map.get(&1, :number)))
                        |> Enum.sort(&(Map.get(&1, :number) <= Map.get(&2, :number)))
                end

            cond do
                is_pid(pid) and syn == ack and number > ack and next.number == number ->
                    [
                        process: instance.namespace,
                        saga: instance.id,
                        status: :running,
                        push: number,
                    ]
                    |> Signal.Logger.info(label: :router)

                    instance.app
                    |> Signal.PubSub.broadcast(instance.uuid, event)
                    %Instance{instance | syn: number, queue: queue, status: :running}

                number > syn  ->
                    [
                        process: instance.namespace,
                        saga: instance.id,
                        status: instance.status,
                        queued: number
                    ]
                    |> Signal.Logger.info(label: :router)
                    %Instance{instance | queue: queue}

                true ->
                    instance
            end
        end

        def stop(%Instance{}=instance, status) do
            %Instance{app: app, ref: ref, uuid: uuid, ack: ack}=instance
            Process.demonitor(ref)
            Supervisor.unregister_child(app, uuid)
            Signal.PubSub.broadcast(app, uuid, status)
            %Instance{instance| pid: nil, ref: nil, syn: ack, status: status}
        end

        def acknowledge(%Instance{}=instance, number) do
            %Instance{ack: ack, syn: syn, queue: queue} = instance
            queue = Enum.filter(queue, &(Map.get(&1, :number) > number))

            # Only acknowledge events with greater numbers
            # ie more recent events
            number = if number > ack, do: number, else: ack

            # On init ack syn === 0 
            # sync with ack from
            # process saga
            syn = if syn == 0 and number > syn, do: number, else: syn

            %Instance{instance| syn: syn, ack: number, queue: queue}
        end

    end

    def init(opts) do
        name = Keyword.fetch!(opts, :name)
        uuid = Effect.uuid(@router_namespace, name)
        opts = 
            opts
            |> Keyword.put(:uuid, uuid)
            |> Keyword.put(:instances, %{})
            |> Keyword.put(:processing, [])
            |> Keyword.put_new_lazy(:timeout, fn -> Signal.Timer.seconds(30) end)
        {:ok, struct(__MODULE__, opts), {:continue, :boot}}
    end

    def handle_boot(%Router{}=router) do

        router = 
            router
            |> load_router_instances()
            |> subscribe_router()
            |> track_router()
            
        {:noreply, router, router.timeout}
    end

    def handle_timeout(%Router{}=router) do
        {:noreply, router, :hibernate}
    end

    def handle_down(ref, %Router{instances: instances}=router) do
          instance = 
              instances
              |> Map.values()
              |> Enum.find(fn 
                  %Instance{ref: ^ref} -> 
                      true
                  _ ->
                      false
              end)

        if is_nil(instance) do
            {:noreply, router, router.timeout}
        else

            instance = Instance.stop(instance, :sleeping)

            case instance.queue do
                [] ->
                    instances = Map.put(instances, instance.id, instance)
                    {:noreply, %Router{router|instances: instances}, router.timeout}

                _ ->

                    %Instance{id: id, ack: ack} = instance

                    pid = start_process(router, id, ack) 

                    instance =
                        instance
                        |> Map.from_struct()
                        |> Map.put(:pid, pid)
                        |> Map.to_list()
                        |> Instance.new()

                    sched_next(instance)

                    instances =  Map.put(instances, instance.id, instance)

                    {:noreply, %Router{router|instances: instances}, router.timeout}
            end

        end
    end

    def handle_start({id, number}, %Router{instances: instances}=router) do
        case Map.get(instances, id) do
            nil ->
                {:noreply, router, router.timeout}

            instance ->
                [
                    process: router.name,
                    saga: id,
                    status: :started,
                    start: number,
                ]
                |> Signal.Logger.info(label: :router)

                unless Enum.empty?(instance.queue) do
                    sched_next(instance)
                end
                {:noreply, router, router.timeout}
        end
    end

    def handle_ack({id, number}, %Router{instances: instances}=router) do
        [
            process: router.name,
            saga: id,
            status: :running,
            ack: number,
        ]
        |> Signal.Logger.info(label: :router)

        instance =
            instances
            |> Map.values()
            |> Enum.find(fn 
                %Instance{id: ^id, syn: ^number} -> 
                    true

                _ -> 
                    false
            end)

        router = 
            router
            |> mark_event_as_processed(number)
            |> acknowledge_processed_events()

        case instance do
            nil ->
                {:noreply, router, router.timeout}

            _ ->
                instance = 
                    instance
                    |> Instance.acknowledge(number)

                instances =  
                    instances
                    |> Map.put(instance.id, instance)

                router = 
                    router
                    |> struct(%{instances: instances})

                {:noreply, router, router.timeout}
        end
    end

    def handle_sleep(id, %Router{}=router) do
        %Router{instances: instances}=router

        [
            process: router.name,
            saga: id,
            status: :sleeping,
        ]
        |> Signal.Logger.info(label: :router)

        case Map.get(instances, id) do
            nil ->
                {:noreply, router, router.timeout}

            instance ->
                # Demonitor if process queue is empty
                # else ignor stopped event
                instances =
                    case instance do
                        %Instance{queue: [], syn: syn, ack: ack} when syn === ack ->
                            instance = Instance.stop(instance, :sleeping)
                            Map.put(instances, instance.id, instance)

                        %Instance{queue: _queue} ->
                            sched_next(instance)
                            Map.put(instances, instance.id, instance)
                    end

                {:noreply, %Router{router| instances: instances}, router.timeout}
        end
    end

    def handle_stop(id, %Router{}=router) do
        %Router{instances: instances}=router
        [
            process: router.name,
            saga: id,
            status: :stopped,
        ]
        |> Signal.Logger.info(label: :router)

        case Map.get(instances, id) do
            nil ->
                {:noreply, router, router.timeout}

            instance -> 
                instance =
                    case instance do
                        %{queue: [], pid: pid} when is_pid(pid) ->
                            Instance.stop(instance, :stopped)

                        _ ->
                            instance
                    end

                instances =
                    if instance.status === :stopped do
                        Map.delete(instances, id)
                    else
                        instances
                    end

                router = 
                    router
                    |> struct(%{instances: instances})

                {:noreply, router, router.timeout}
        end
    end

    def handle_alive(id, %Router{instances: instances}=router) do
        found =
            case Map.get(instances, id) do
                %Instance{} ->  
                    true
                _ -> 
                    false
            end
        {:reply, found, router, router.timeout}
    end

    def handle_event(%Event{}=event, %Router{}=router) do
        %Router{
            app: app, 
            name: namespace,
            module: module, 
            instances: instances, 
        } = router

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

        instance =  Map.get(instances, id)
        [
            process: router.name,
            routing: event.topic,
            number: event.number,
            handle: reply,
        ]
        |> Signal.Logger.info(label: :router)

        target =
            case {action, instance} do
                {:start, %Instance{}}  ->
                    instance

                {:apply, %Instance{}} ->
                    instance

                {:start, nil}  ->
                    pid = start_process(router, id)
                    opts = [
                        id: id, 
                        app: app, 
                        pid: pid, 
                        namespace: namespace
                    ]
                    Instance.new(opts)

                _ ->  
                    nil
            end

        if target do

            instance = 
                router
                |> wake_process(target)
                |> Instance.push_event(event)

            instances = Map.put(instances, instance.id, instance)

            router = 
                # If event has been pushed to
                # saga then add the event number
                # to processing list
                if instance.syn === event.number do
                    mark_event_as_processing(router, event.number, false)
                else
                    router
                end
                |> struct(%{instances: instances})

            {:noreply, router, router.timeout}
        else
            router = 
                router
                |> mark_event_as_processing(event.number, true)
                |> struct(%{instances: instances})
                |> acknowledge_processed_events()

            {:noreply, router, router.timeout}
        end
    end

    def handle_next(id, %Router{instances: instances}=router) do

        case Map.get(instances, id) do
            nil ->
                nil

            %Instance{queue: []} ->
                nil

            %Instance{queue: [%Event{}=event|_]} ->
                send(self(), event)
        end

        {:noreply, router, router.timeout}
    end

    defp acknowledge_processed_events(%Router{processing: []}=router) do
        router
    end

    defp acknowledge_processed_events(%Router{processing: [{_, false}|_]}=router) do
        router
    end

    defp acknowledge_processed_events(%Router{}=router) do
        %Router{
            app: application, 
            consumer:  consumer,
            processing: [{number, true}| processing]
        } = router

        if number > consumer.ack  do
            [process: router.name, ack: number]
            |> Signal.Logger.info(label: :router)

            consumer = 
                application
                |> Broker.acknowledge(consumer, number)

            %Router{router| 
                consumer: consumer,
                processing: processing,
            }
            |> acknowledge_processed_events()
        else
            router
        end
    end

    defp sched_next(%Instance{id: id}) do
        Process.send(self(), {:next, id}, [])
    end

    defp start_process(router, id, index \\ 0)
    defp start_process(%Router{}=router, id, index) do
        suuid = Effect.uuid(router.name, id)
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

    defp wake_process(%Router{}=router, %Instance{pid: nil, status: :sleeping}=inst) do
        pid = start_process(router, inst.id, inst.ack)
        inst
        |> Map.from_struct() 
        |> Map.put(:pid, pid)
        |> Map.put(:status, :running)
        |> Map.to_list()
        |> Instance.new()
    end

    defp wake_process(%Router{}, %Instance{}=instance) do
        instance
    end

    defp subscribe_router(%Router{}=router) do
        %Router{
            app: application, 
            name: name, 
            uuid: uuid,
            topics: topics, 
        }=router

        subopts = [topics: topics, start: :cursor, track: true]

        consumer = 
            application
            |> Broker.subscribe(name, subopts)

        application
        |> Signal.PubSub.subscribe(uuid)

        %Router{router | consumer: consumer}
    end

    defp load_router_instances(%Router{}=router) do
        %Router{app: app, name: name, module: module}=router

        common = [app: app, namespace: name, module: module]

        instances =
            app
            |> Signal.Store.Adapter.list_effects(name)
            |> Enum.reduce(router.instances, fn %Effect{data: data}, instances -> 
                id = Map.get(data, "id")
                ack = Map.get(data, "ack")
                status = Map.get(data, "status")
                buffer = Map.get(data, "buffer")
                actions = Map.get(data, "actions")
                instance =
                    case {actions, buffer} do
                        # No events and actions
                        # and process is not stopping
                        # then assume process as sleeping
                        {[], []} when status !== "stop" ->
                            common
                            |> Keyword.merge([id: id, ack: ack])
                            |> Instance.new()

                          _ ->
                            pid = start_process(router, id, ack)
                            common
                            |> Keyword.merge([id: id, ack: ack, pid: pid])
                            |> Instance.new()
                    end
                Map.put(instances, instance.id, instance)
            end)

        %Router{router | instances: instances}
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

    defp mark_event_as_processing(%Router{}=router, number, ack) do
        processing =
            router.processing
            |> Enum.concat([{number, ack}])
            |> Enum.uniq_by(&(elem(&1,0)))
            |> Enum.sort(&(elem(&1,0) <= elem(&2,0)))

        %Router{router| processing: processing}
    end

    defp mark_event_as_processed(%Router{}=router, number) do
        %Router{processing: processing} = router
        index = Enum.find_index(processing, &(elem(&1, 0) === number))

        processing =
            if index do
                List.replace_at(processing, index, {number, true})
            else
                processing
            end
        %Router{router| processing: processing}
    end


end
