defmodule Signal.Execution.Queue do
    use GenServer

    alias Signal.Execution.Queue
    alias Signal.Command.Executor

    defstruct [:application, :id, type: :default, timeout: :infinity]

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
    def handle_call({:execute, command, params, opts}, _from, %Queue{}=state) do
        {:reply, execute(command, params, opts), state}
    end

    def handle(application, command, params, opts \\ []) do

        queue = Signal.Queue.queue(command)

        case queue do
            {type, id} when is_atom(type) and is_binary(id) ->
                application
                |> Signal.Execution.Supervisor.prepare_queue(id, type)
                |> GenServer.call({:execute, command, params, opts})

            nil ->
                execute(command, params, opts)

            {:error, error} -> 
                {:error, error}

            message -> 
                {:error, message}
        end
    end

    def execute(command, params, _opts \\ []) do
        Executor.execute(command, params)
    end

end

