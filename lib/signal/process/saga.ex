defmodule Signal.Process.Saga do
    use GenServer, restart: :transient

    alias Signal.Event
    alias Signal.Codec
    alias Signal.Result
    alias Signal.Process.Saga
    alias Signal.Event.Metadata
    alias Signal.Process.Supervisor

    defstruct [
        :id, 
        :app,
        :uuid,
        :state, 
        :start,
        :module, 
        :domain,
        :channel,
        ack: 0, 
        version: 0,
        timeout: 5000,
        status: :start,
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
        {:ok, struct(__MODULE__, opts), {:continue, :load_effect}}
    end

    def start(app, {id, module}, opts \\ []) do
        Supervisor.prepare_saga(app, {id, module}, opts)    
    end

    @impl true
    def handle_continue(:load_effect, %Saga{}=saga) do
        saga =
            case Signal.Store.Adapter.get_effect(saga.app, saga.uuid) do
                %Signal.Effect{}=effect->
                    load_saga_state(saga, effect)

                _ -> 
                    state = 
                        saga.module
                        |> Kernel.apply(:init, [saga.id])

                    %Saga{saga| state: state}
            end

        Signal.PubSub.subscribe(saga.app, saga.uuid)

        router_push(saga, {saga.status, saga.id, saga.start})
        [
            app: saga.app,
            sid: saga.id,
            spid: saga.uuid,
            domain: saga.domain,
            status: saga.status,
            loaded: saga.version,
            timeout: saga.timeout,
        ]
        |> Signal.Logger.info(label: :saga)
        {:noreply, saga, saga.timeout}
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

        [
            app: saga.app,
            type: module,
            id: saga.id,
            cause: event.topic,
            dispatch: command.__struct__,
        ]
        |> Signal.Logger.info(label: :saga)

        case execute(command, saga, opts) do
            {:ok, %Result{}}->
                saga = 
                    saga
                    |> acknowledge_event(number, :running) 
                {:noreply, saga, saga.timeout}

            {:error, error}->
                args = [{command, error}, event, state]
                reply = Kernel.apply(module, :handle_error, args) 
                handle_reply(saga, event, reply)

            error ->
                {:stop, error, saga}
        end
    end

    @impl true
    def handle_info(:timeout, %Saga{}=saga) do
        {:noreply, saga, :hibernate}
    end

    @impl true
    def handle_info({:init, index}, %Saga{}=saga) when is_number(index) do
        %Saga{id: id, module: module, ack: ack, version: version} = saga
        saga = 
            cond do
                ack == 0 and version == 0 and index > ack ->
                    state = Kernel.apply(module, :init, [id])
                    %Saga{saga | ack: 0, state: state, version: 0}

                true ->
                    saga
            end
            
        saga =
            saga
            |> acknowledge_event(saga.ack, :running)

        {:noreply, saga, saga.timeout}
    end

    @impl true
    def handle_info({_action, %Event{number: number}}, %Saga{ack: ack}=saga)
    when number <= ack do
        {:noreply, saga}
    end

    @impl true
    def handle_info({action, %Event{}=event}, %Saga{}=saga) 
    when action in [:apply, :start] do

        %Saga{module: module, state: state} = saga

        [
            app: saga.app,
            type: module,
            sid: saga.id,
            status: :running,
            apply: event.topic,
            number: event.number,
        ]
        |> Signal.Logger.info(label: :saga)

        reply = Kernel.apply(module, :handle_event, [Event.data(event), state])

        handle_reply(saga, event, reply)
    end

    defp execute(command, %Saga{app: app}, opts) do
        Kernel.apply(app, :dispatch, [command, opts])
    end

    defp acknowledge_event(%Saga{id: id, version: version}=saga, number, status) do
        router_push(saga, {:ack, id, number, status})
        %Saga{saga | ack: number, status: status, version: version + 1}
    end

    defp save_saga_state(%Saga{app: app, ack: ack}=saga) do
        effect = create_effect(saga, ack)
        :ok =
            app
            |> Signal.Store.Adapter.save_effect(effect)
        saga
    end

    defp shutdown_saga(%Saga{app: app, domain: domain, id: id}=saga) do
        process_uuid  = Signal.Effect.uuid(domain, id)
        :ok = Signal.Store.Adapter.delete_effect(app, process_uuid)
        saga
    end

    defp create_effect(%Saga{id: id, state: state, domain: domain}, ack) do
        {:ok, data} = Codec.encode(state)
        [id: id, domain: domain, data: data, number: ack]
        |> Signal.Effect.new()
    end

    defp handle_reply(%Saga{}=saga, %Event{number: number}=event, reply) do
        case reply do 
            {:dispatch, command, state} ->
                Process.send(self(), {:execute, event, command}, []) 
                {:noreply, %Saga{saga | state: state}}

            {:ok, state} ->
                saga = 
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :running)
                    |> save_saga_state()
                {:noreply, saga, saga.timeout}

            {:sleep, state} ->
                saga = 
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :running)
                    |> save_saga_state()
                {:noreply, saga, :hibernate}

            {:hibernate, state} ->
                saga = 
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :sleep)
                    |> save_saga_state()
                {:noreply, saga}


            {:stop, state} ->
                [
                    app: saga.app,
                    type: saga.module,
                    sid: saga.id,
                    event: event.topic,
                    status: :stopped,
                    number: event.number,
                ]
                |> Signal.Logger.info(label: :saga)

                saga =
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :stop)
                    |> shutdown_saga()

                {:stop, :normal, saga}
        end
    end

    defp load_saga_state(%Saga{}=saga, %Signal.Effect{data: data}) do
        %{
            "ack" => ack, 
            "state" => payload, 
            "status" => status,
            "version" => version, 
        } = data

        status = String.to_existing_atom(status)

        {:ok, state} = 
            saga.module
            |> struct([])
            |> Codec.load(payload)
        %Saga{saga | state: state, ack: ack, version: version, status: status}
    end

    def create_saga_effect(%Saga{}=saga) do
        %Saga{
            id: id, 
            ack: ack, 
            state: state, 
            status: status,
            domain: domain,
            version: version, 
        } = saga

        status = String.to_existing_atom(status)

        {:ok, payload} = Codec.encode(state)

        data = %{
            "id" => id,
            "ack" => ack, 
            "domain" => domain,
            "state" => payload, 
            "status" => status,
            "version" => version, 
        } 

        uuid = Signal.Effect.uuid(domain, id)

        [uuid: uuid, data: data]
        |> Signal.Effect.new()
    end

    def router_push(%Saga{}=saga, payload) do
        Signal.PubSub.broadcast(saga.app, saga.channel, payload)
    end

    @impl true
    def terminate(_, _) do
        :ok
    end

end
