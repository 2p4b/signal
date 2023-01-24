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

        %Saga{app: app, module: module, id: id}=saga

        {application, _tenant} = app

        snapshot_id = identity(saga)

        {version, state} =
            case Signal.Store.Adapter.get_snapshot(application, snapshot_id) do
                %{payload: %{"data" => data, "ack" => ack}}->
                    {:ok, state} = 
                        module
                        |> struct([])
                        |> Codec.load(data)

                    {ack, state}

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

        snapshots = 
            saga
            |> snapshot(number)
            |> List.wrap()

        opts = [
            snapshots: snapshots,
            causation_id: uuid,
            correlation_id: correlation_id
        ]
        log(saga, "dispatch: #{command.__struct__}")
        case execute(command, saga, opts) do
            {:ok, %Result{}}->
                {:noreply, acknowledge(saga, number, :running)}

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

    defp acknowledge(%Saga{id: id, module: router}=saga, number, status) do
        GenServer.cast(router, {:ack, id, number, status})
        %Saga{saga | ack: number, status: status}
    end

    defp checkpoint(%Saga{app: app, ack: ack}=saga) do
        {application, _tenant}  = app

        snapshot  =
            saga
            |> snapshot(ack)

        application
        |> Signal.Store.Adapter.record_snapshot(snapshot)

        saga
    end

    defp shutdown(%Saga{app: app}=saga) do
        {application, _tenant}  = app

        snapshot_id = 
            saga
            |> identity()

        application
        |> Signal.Store.Adapter.delete_snapshot(snapshot_id)

        saga
    end

    defp identity(%Saga{id: id, module: module}) do
        {id, Signal.Helper.module_to_string(module)}
    end

    defp snapshot(%Saga{state: state}=saga, ack) do
        {:ok, data} = Codec.encode(state)

        payload = %{"data" => data, "ack" => ack}

        saga
        |> identity()
        |> Snapshot.new(payload, version: 1)
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
                    |> acknowledge(number, :running)
                    |> checkpoint()
                {:noreply, saga}

            {:sleep, state} ->
                saga = 
                    %Saga{ saga | state: state}
                    |> acknowledge(number, :sleeping)
                    |> checkpoint()

                {:noreply, saga}


            {:shutdown, state} ->
                saga =
                    %Saga{ saga | state: state}
                    |> shutdown()
                    |> acknowledge(number, :shutdown)

                log(saga, "shutdown")
                {:stop, :shutdown, saga}
        end
    end

    @impl true
    def terminate(_, _) do
        :ok
    end

end
