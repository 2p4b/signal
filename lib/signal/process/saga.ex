defmodule Signal.Process.Saga do
    use GenServer

    alias Signal.Codec
    alias Signal.Result
    alias Signal.Process.Saga
    alias Signal.Events.Event
    alias Signal.Process.Supervisor
    alias Signal.Events.Event.Metadata

    defstruct [
        :app, :state, :store, :id, :ack, :module, 
        version: 0, status: :running
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

    def resume(app, {module, id}, ack, ensure \\ false) do
        method = if ensure do :resume! else :resume end
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
        saga = %Saga{saga | version: position, ack: position, state: state}
        {:reply, {:ok, state}, saga}
    end

    @impl true
    def handle_call({:start!, position}, from, %Saga{id: id, version: version}=saga) do
        if version == 0 do
            handle_call({:start, position}, from, saga)
        else
            {:stop, {:process_already_started, id}, saga}
        end
    end

    @impl true
    def handle_call({:resume, ack}, _from, %Saga{version: version}=saga) do
        acknowledge(saga, version)
        {:reply, {:ok, version}, saga}
    end

    @impl true
    def handle_call({:resume!, ack}, from, %Saga{id: id, version: version}=saga) do
        if version > 0 do
            handle_call({:resume, ack}, from, saga)
        else
            {:stop, {:process_not_started, id}, saga}
        end
    end

    @impl true
    def handle_info({:stop, %Event{}=event}, %Saga{}=saga) do

        IO.inspect(event.number, label: "[saga] recv stop") 
        %Saga{module: module, state: state} = saga

        case Kernel.apply(module, :stop, [Event.payload(event), state]) do
            {:ok, state} ->
                %Saga{ saga | state: state} 
                |> checkpoint()

            _ -> 
                nil
        end
        exit(:normal)
        {:noreply, saga}
    end

    @impl true
    def handle_info(:init, %Saga{}=saga) do

        %Saga{app: app, module: module, store: store}=saga

        {version, state} = 
            case store.get_state(app, identity(saga), :max) do
                {version, state} when is_integer(version) ->
                    {version, Codec.load(struct(module, []), state)}
                _ ->
                    {0, nil}
            end
        {:noreply, %Saga{saga | version: version, state: state} }
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
                {:noreply, acknowledge(saga, number)}

            {:error, error}->
                case Kernel.apply(module, :error, [command, error, state]) do
                    {:dispatch, command, state} ->
                        Process.send(self(), {:execute, command, meta}, []) 
                        {:noreply, %Saga{saga | state: state}}

                    {:ok, state} ->
                        saga = 
                            %Saga{ saga | state: state}
                            |> acknowledge(number)
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
    def handle_info({:halt, %Event{number: number}=event}, %Saga{}=saga) do
        IO.inspect(number, label: "[saga] recv halt") 
        %Saga{module: module, state: state} = saga

        case Kernel.apply(module, :halt, [Event.payload(event), state]) do
            {:continue, state} ->
                saga = 
                    %Saga{ saga | state: state} 
                    |> acknowledge(number, :continue) 
                {:noreply, saga}

            {:stop, state} ->
                saga = 
                    %Saga{ saga | state: state} 
                    |> acknowledge(number, :stop) 
                    |> checkpoint()

                handle_info({:stop, event}, saga)

                {:noreply, saga}
        end
    end

    @impl true
    def handle_info(%Event{number: number}=event, %Saga{}=saga) do
        case Kernel.apply(saga.module, :apply, [Event.payload(event), saga.state]) do
            {:dispatch, command, state} ->
                Process.send(self(), {:execute, command, Event.metadata(event)}, []) 
                {:noreply, %Saga{ saga | state: state}}


            {:ok, state} ->
                saga = 
                    %Saga{ saga | state: state}
                    |> acknowledge(number)
                    |> checkpoint()
                {:noreply, saga}
        end
    end

    defp execute(command, %Saga{app: {app_module, app_name}}, opts) do
        opts = Keyword.merge(opts, [app: app_name])
        Kernel.apply(app_module, :dispatch, [command, opts])
    end

    defp acknowledge(%Saga{id: id, module: module}=saga, number, status \\ nil) do
        ack = 
            if is_nil(status) do
                {:ack, id, number}
            else
                {:ack, id, number, status}
            end

        module
        |> GenServer.whereis()
        |> Process.send(ack, [])

        %Saga{saga | version: number}
    end

    defp checkpoint(%Saga{}=saga) do
        %Saga{app: app, state: state, store: store, version: version} = saga
        args = [app, identity(saga), version, Codec.encode(state)]
        {:ok, ^version} = Kernel.apply(store, :set_state, args) 
        saga
    end

    defp identity(%Saga{id: id, module: module}) do
        Signal.Helper.module_to_string(module) <> ":" <> id
    end

end
