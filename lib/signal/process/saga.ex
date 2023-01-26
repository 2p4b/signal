defmodule Signal.Process.Saga do
    use GenServer, restart: :transient

    alias Signal.Event
    alias Signal.Codec
    alias Signal.Result
    alias Signal.Snapshot
    alias Signal.Process.Saga
    alias Signal.Event.Metadata
    alias Signal.Process.Supervisor

    require Logger

    defstruct [
        :id, 
        :app, 
        :state, 
        :module, 
        ack: 0, 
        :namespace,
        status: :running,
        version: 0,
    ]

    @doc """
    Starts a new process.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        Process.send(self(), :load, [])
        {:ok, struct(__MODULE__, opts)}
    end

    def start(app, {id, module}, index \\ 0) do
        saga = Supervisor.prepare_saga(app, {id, module})    
        saga
        |> GenServer.whereis()
        |> GenServer.cast({:init, index})
        saga
    end

    def position(app, {id, module}) do
        Supervisor.prepare_saga(app, {id, module})    
        |> GenServer.call(:position, 5000)
    end

    @impl true
    def handle_call(:position, _from, %Saga{version: version}=saga) do
        {:reply, version, saga}
    end

    @impl true
    def handle_info(:load, %Saga{}=saga) do

        %Saga{app: app, module: module, id: id, namespace: namespace}=saga

        {application, _tenant} = app

        {version, state} =
            case Signal.Store.Adapter.get_effect(application, namespace, id) do
                %Signal.Effect{object: object, number: number}->
                    {:ok, state} = 
                        module
                        |> struct([])
                        |> Codec.load(data)

                    {number, state}

                _ -> 
                    initial_state = 
                        module
                        |> Kernel.apply(:init, [id])

                    {0, initial_state} 
            end

        log(saga, "starting from: #{version}")

        saga = struct(saga, %{ack: version, version: version, state: state})

        {:noreply, saga}
    end

    @impl true
    def handle_info({:execute, %Event{}=event, command}, %Saga{}=saga) 
    when is_struct(command) do
        %Saga{state: state, module: module} = saga
        %Metadata{
            uuid: uuid, 
            number: number, 
            correlation_id: correlation_id
        } = Event.metadata(event)

        effects = 
            saga
            |> create_effect(number)
            |> List.wrap()

        opts = [
            effects: effects,
            causation_id: uuid,
            correlation_id: correlation_id
        ]
        log(saga, "dispatch: #{command.__struct__}")
        case execute(command, saga, opts) do
            {:ok, %Result{}}->
                {:noreply, acknowledge_event_status(saga, number, :running)}

            {:error, error}->
                params = %{
                    error: error,
                    event: Event.payload(event),
                    metadata: Event.metadata(event)
                }
                reply = Kernel.apply(module, :error, [command, params, state]) 
                handle_reply(saga, event, reply)

            error ->
                {:stop, error, saga}
        end
    end

    @impl true
    def handle_cast({:init, index}, %Saga{}=saga) when is_number(index) do
        %Saga{id: id, module: module, ack: ack, version: version} = saga
        saga = 
            cond do
                ack == 0 and version == 0 and index > ack ->
                    state = Kernel.apply(module, :init, [id])
                     %Saga{saga | 
                        ack: 0, 
                        state: state, 
                        status: :running,
                        version: 0, 
                    }

                index <= ack ->
                    struct(saga, status: :running)

                true ->
                    saga
            end

        GenServer.cast(module, {:ack, id, saga.ack, saga.status})
        {:noreply, saga}
    end

    @impl true
    def handle_cast({_action, %Event{number: number}}, %Saga{ack: ack}=saga)
    when number <= ack do
        {:noreply, saga}
    end

    @impl true
    def handle_cast({action, %Event{}=event}, %Saga{}=saga) 
    when action in [:apply, :start] do

        %Event{topic: topic}=event
        %Saga{module: module, state: state} = saga

        log(saga, "applying: #{inspect(topic)}")

        metadata = Event.metadata(event)

        reply = Kernel.apply(module, :apply, [Event.payload(event), metadata, state])

        handle_reply(saga, event, reply)
    end

    defp execute(command, %Saga{app: app}, opts) do
        {application, _tenant}  = app
        Kernel.apply(application, :dispatch, [command, opts])
    end

    defp acknowledge_event_status(%Saga{id: id, module: router}=saga, number, status) do
        GenServer.cast(router, {:ack, id, number, status})
        %Saga{saga | ack: number, status: status}
    end

    defp save_saga_state(%Saga{app: app, ack: ack}=saga) do
        {application, _tenant}  = app
        effect = create_effect(saga, ack)
        application
        |> Signal.Store.Adapter.save_effect(effect)

        saga
    end

    defp shutdown_saga(%Saga{app: app, namespace: namespace, id: id}=saga) do
        {application, _tenant}  = app
        application
        |> Signal.Store.Adapter.delete_effect(namespace, id)
        saga
    end

    defp create_effect(%Saga{id: id, state: state, namespace: namespace}=saga, ack) do
        {:ok, object} = Codec.encode(state)
        [id: id, namespace: namespace, object: object, number: ack]
        |> Signal.Effect.new()
    end

    defp log(%Saga{module: module, id: id}, info) do
        info = """ 

        [SAGA] #{inspect(module)} #{id}
        #{info}
        """
        Logger.info(info)
    end


    defp handle_reply(%Saga{}=saga, %Event{number: number}=event, reply) do
        case reply do 
            {:dispatch, command, state} ->
                Process.send(self(), {:execute, event, command}, []) 
                {:noreply, %Saga{saga | state: state}}

            {:ok, state} ->
                saga = 
                    %Saga{ saga | state: state}
                    |> acknowledge_event_status(number, :running)
                    |> save_saga_state()
                {:noreply, saga}

            {:sleep, state} ->
                saga = 
                    %Saga{ saga | state: state}
                    |> acknowledge_event_status(number, :sleeping)
                    |> save_saga_state()

                {:noreply, saga}


            {:shutdown, state} ->
                saga =
                    %Saga{ saga | state: state}
                    |> shutdown_saga()
                    |> acknowledge_event_status(number, :shutdown)

                log(saga, "shutdown")
                {:stop, :shutdown, saga}
        end
    end

    @impl true
    def terminate(_, _) do
        :ok
    end

end
