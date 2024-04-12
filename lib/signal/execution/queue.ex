defmodule Signal.Execution.Queue do
    use GenServer, restart: :transient
    use Signal.Telemetry

    alias Signal.Command.Handler
    alias Signal.Execution.Queue

    defstruct [:app, :id, type: :default, timeout: (5 * 1000)]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        {:ok, struct(__MODULE__, opts )}
    end

    @impl true
    def handle_call({:execute, command, assigns, opts}, _from, %Queue{}=state) do
        {:reply, execute(command, assigns, opts), state, state.timeout}
    end

    @impl true
    def terminate(reason, state) do
        Map.from_struct(state)
        |> Map.to_list()
        |> Enum.concat([shutdown: reason])
        |> Signal.Logger.info(label: :queue)
        :shutdown
    end

    def handle(app, command, assigns, opts \\ []) do
        qid = Signal.Queue.queue(command)
        opts = opts ++ [app: app]
        case qid  do
            id when is_binary(id) ->
                type = Keyword.get(opts, :type, :default)
                app
                |> Signal.Execution.Supervisor.prepare_queue(id, type)
                |> GenServer.call({:execute, command, assigns, opts})

            nil ->
                execute(command, assigns, opts)

            {:error, error} -> 
                {:error, error}

            message -> 
                {:error, message}
        end
    end

    def execute(command, assigns, opts \\ []) do
        meta = [
            app: Keyword.fetch!(opts, :app),
            command: command.__struct__, 
            queue: Signal.Queue.queue(command), 
        ]

        metadata = Enum.into(meta, %{})
        start = telemetry_start(:execute, %{}, %{})

        try do
            results = Handler.execute(command, assigns)
            telemetry_stop(:execute, start, metadata, %{})
            Signal.Logger.info(meta, label: :queue)
            results

        rescue
            raised -> {:error, {:reraise, raised}}

        catch
            thrown -> 
                telemetry_exception(:execute, start, thrown, __STACKTRACE__, metadata, %{})
                {:error, {:rethrow, thrown}}
        end
    end

end

