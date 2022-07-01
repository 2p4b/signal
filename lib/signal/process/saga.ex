defmodule Signal.Process.Saga do
    use GenServer

    alias Signal.Codec
    alias Signal.Result
    alias Signal.Snapshot
    alias Signal.Process.Saga
    alias Signal.Stream.Event
    alias Signal.Process.Supervisor
    alias Signal.Stream.Event.Metadata

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

        {application, tenant} = app

        {version, state} =
            case application.snapshot(identity(saga), tenant: tenant) do
                nil -> 
                    initial_state = Kernel.apply(module, :init, [id])
                    {0, initial_state} 
                %{version: version, data: data}->
                    state = Codec.load(struct(module, []), data)
                    {version, state}
            end

        log(saga, "starting from: #{version}")

        saga = struct(saga, %{ack: version, version: version, state: state})

        {:noreply, saga}
    end

    @impl true
    def handle_info({:execute, %Event{}=event, command}, %Saga{}=saga) do
        %Saga{state: state, module: module} = saga
        %Metadata{
            number: number, 
            uuid: uuid, 
            correlation_id: 
            correlation_id
        } = Event.metadata(event)

        snapshots = 
            saga
            |> identity()
            |> Snapshot.new(Codec.encode(state), version: number)
            |> List.wrap()

        opts = [
            snapshots: snapshots,
            causation_id: uuid,
            correlation_id: correlation_id
        ]
        case execute(command, saga, opts) do
            %Result{}->
                {:noreply, acknowledge(saga, number, :running)}

            {:error, error}->
                params = %{
                    error: error,
                    event: Event.payload(event),
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

        %Event{type: type}=event
        %Saga{module: module, state: state} = saga

        log(saga, "applying: #{inspect(type)}")

        reply = Kernel.apply(module, :apply, [Event.payload(event), state])

        handle_reply(saga, event, reply)
    end

    defp execute(command, %Saga{app: app}, opts) do
        {application, tenant}  = app
        opts = Keyword.merge(opts, [app: tenant])
        Kernel.apply(application, :dispatch, [command, opts])
    end

    defp acknowledge(%Saga{id: id, module: router}=saga, number, status) do
        GenServer.cast(router, {:ack, id, number, status})
        %Saga{saga | ack: number, status: status}
    end

    defp checkpoint(%Saga{app: app}=saga) do
        {application, tenant}  = app
        saga
        |> snapshot()
        |> application.record([tenant: tenant])
        saga
    end

    defp shutdown(%Saga{app: app}=saga) do
        {application, tenant}  = app
        saga
        |> identity()
        |> application.purge([tenant: tenant])
        saga
    end

    defp identity(%Saga{id: id, module: module}) do
        {Signal.Helper.module_to_string(module), id}
    end

    defp snapshot(%Saga{state: state, ack: number}=saga) do
        saga
        |> identity()
        |> Snapshot.new(Codec.encode(state), version: number)
    end

    defp log(%Saga{module: module, id: id}, info) do
        info = """ 

        [SAGA] #{inspect(module)} 
               id: #{id}
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
                %Saga{ saga | state: state}
                |> shutdown()
                |> acknowledge(number, :shutdown)
                {:stop, :shutdown, saga}
        end
    end

    @impl true
    def terminate(_, _) do
        :ok
    end

end
