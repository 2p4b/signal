defmodule Signal.Process.Saga do
    use GenServer

    alias Signal.Codec
    alias Signal.Result
    alias Signal.Process.Saga
    alias Signal.Stream.Event
    alias Signal.Process.Supervisor
    alias Signal.Stream.Event.Metadata

    defstruct [
        :app, :state, :store, :id, :module, :status,
        ack: 0, version: 0
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

    def start(app, {module, id}) do
        Supervisor.prepare_saga(app, {module, id})    
    end

    def position(app, {module, id}) do
        Supervisor.prepare_saga(app, {module, id})    
        |> GenServer.call(:position, 5000)
    end

    def continue(app, {module, id}, ack, ensure \\ false) do
        method = if ensure do :continue! else :continue end
        Supervisor.prepare_saga(app, {module, id})    
        |> GenServer.call({method, ack}, 5000)
    end

    @impl true
    def handle_call(:position, _from, %Saga{version: version}=saga) do
        {:reply, version, saga}
    end


    @impl true
    def handle_call({:start, position}, _from, %Saga{id: id, module: module}=saga) do
        state = Kernel.apply(module, :init, [id])
        saga = %Saga{saga | 
            version: 0, 
            ack: position, 
            state: state, 
            status: :running
        }
        {:reply, {saga.ack, saga.status}, saga}
    end

    @impl true
    def handle_call({:start!, position}, from, %Saga{id: id, status: status}=saga) do
        if is_nil(status) do
            handle_call({:start, position}, from, saga)
        else
            {:stop, {:process_already_started, id}, saga}
        end
    end

    @impl true
    def handle_call({:continue, _continue_ack}, _from, %Saga{ack: saga_ack}=saga) do
        acknowledge(saga, saga_ack, :running)
        {:reply, {saga_ack, saga.status}, saga}
    end

    @impl true
    def handle_call({:continue!, ack}, from, %Saga{id: id, status: status}=saga) do
        if is_nil(status) do
            {:stop, {:process_not_started, id}, saga}
        else
            handle_call({:continue, ack}, from, saga)
        end
    end

    @impl true
    def handle_info(:init, %Saga{}=saga) do

        %Saga{app: app}=saga

        updates = %{version: 0} 

        {:noreply, struct(saga, updates)}
    end

    @impl true
    def handle_info({:execute, command, %Metadata{}=meta}, %Saga{}=saga) do
        %Saga{state: state, module: module} = saga
        %Metadata{number: number, uuid: uuid, correlation_id: correlation_id} = meta

        snapshot = {identity(saga), number, Codec.encode(state)}
        opts = [
            states: [snapshot],
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
    def handle_cast({:halt, %Event{number: number}=event}, %Saga{}=saga) do
        %Saga{module: module, state: state} = saga

        case Kernel.apply(module, :halt, [Event.payload(event), state]) do

            {:resume, state} ->
                saga = 
                    %Saga{ saga | state: state} 
                    |> acknowledge(number, :running) 
                {:noreply, saga}

            {:stop, state} ->
                saga = 
                    %Saga{ saga | state: state} 
                    |> acknowledge(number, :stopped) 

                stop_process(saga)
                {:noreply, saga}
        end
    end

    @impl true
    def handle_cast({action, %Event{number: number}=event}, %Saga{}=saga) 
    when action in [:apply, :start, :start!, :apply!]do

        %Saga{module: module, state} = saga

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
        Signal.Helper.module_to_string(module) <> ":" <> id
    end

    defp stop_process(%Saga{}) do
        exit(:normal)
    end


end
