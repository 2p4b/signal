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
        status: :init,
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
        Process.send(self(), :init, [])
        {:ok, struct(__MODULE__, opts)}
    end

    def start(app, {module, id}, index \\ 0) do
        saga = Supervisor.prepare_saga(app, {module, id})    
        saga
        |> GenServer.whereis()
        |> GenServer.cast({:start, index})
        saga
    end

    def position(app, {module, id}) do
        Supervisor.prepare_saga(app, {module, id})    
        |> GenServer.call(:position, 5000)
    end


    @impl true
    def handle_call(:position, _from, %Saga{version: version}=saga) do
        {:reply, version, saga}
    end


    @impl true
    def handle_info(:init, %Saga{}=saga) do

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
    def handle_info({:execute, command, %Metadata{}=meta}, %Saga{}=saga) do
        %Saga{state: state, module: module} = saga
        %Metadata{number: number, uuid: uuid, correlation_id: correlation_id} = meta

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
                case Kernel.apply(module, :error, [command, error, state]) do
                    {:dispatch, command, state} ->
                        Process.send(self(), {:execute, command, meta}, []) 
                        {:noreply, %Saga{saga | state: state}}

                    {:ok, state} ->
                        saga = 
                            %Saga{ saga | state: state}
                            |> acknowledge(number, :running)
                            |> checkpoint()
                        {:noreply, saga}

                    error ->
                        {:stop, error, saga}
                end

            error ->
                {:stop, error, saga}
        end
    end

    @impl true
    def handle_cast({:start, index}, %Saga{status: :init}=saga) do
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
    def handle_cast({_action, %Event{number: number}}, %Saga{version: version}=saga)
    when number < version do
        {:noreply, saga}
    end

    @impl true
    def handle_cast({:stop, %Event{type: type, number: number}=event}, %Saga{}=saga) do
        %Saga{module: module, state: state} = saga

        log(saga, "stopping: #{inspect(type)}")
        case Kernel.apply(module, :stop, [Event.payload(event), state]) do

            {:ok, state} ->
                log(saga, "stopped")

                saga = 
                    saga
                    |> struct(%{ack: number, state: state})
                    |> checkpoint()

                stop_process(saga)
                {:noreply, saga}
        end
    end

    @impl true
    def handle_cast({action, %Event{}=event}, %Saga{status: :running}=saga) 
    when action in [:apply, :start, :start!, :apply!] do

        %Event{type: type, number: number}=event
        %Saga{module: module, state: state} = saga

        log(saga, "applying: #{inspect(type)}")
        case Kernel.apply(module, :apply, [Event.payload(event), state]) do
            {:dispatch, command, state} ->
                Process.send(self(), {:execute, command, Event.metadata(event)}, []) 
                {:noreply, %Saga{ saga | state: state}}


            {:ok, state} ->
                saga = 
                    %Saga{ saga | state: state}
                    |> acknowledge(number, :running)
                {:noreply, saga}
        end
    end

    defp execute(command, %Saga{app: app}, opts) do
        {application, tenant}  = app
        opts = Keyword.merge(opts, [app: tenant])
        Kernel.apply(application, :dispatch, [command, opts])
    end

    defp acknowledge(%Saga{id: id, module: router}=saga, number, status) do

        GenServer.cast(router, {:ack, id, number, status})

        %Saga{saga | ack: number, status: status}
        |> inc_version()
        |> checkpoint()
    end

    defp inc_version(%Saga{version: version}=saga) do
        %Saga{saga | version: version + 1}
    end

    defp checkpoint(%Saga{}=saga) do
        saga
    end

    defp identity(%Saga{id: id, module: module}) do
        {Signal.Helper.module_to_string(module), id}
    end

    defp stop_process(%Saga{}) do
        exit(:normal)
    end

    defp log(%Saga{module: module, id: id}, info) do
        info = """ 

        [SAGA] #{inspect(module)} 
               id: #{id}
               #{info}
        """
        Logger.info(info)
    end


end
