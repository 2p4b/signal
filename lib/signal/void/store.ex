defmodule Signal.Void.Store do
    use Supervisor

    alias Signal.Snapshot
    alias Signal.Void.Repo
    alias Signal.Void.Broker

    @behaviour Signal.Store

    def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
    end

    def init(init_arg) do
        children = [Repo, Broker]
        opts = [strategy: :one_for_one, name: __MODULE__]
        Supervisor.init(children, opts)
    end

    @impl true
    def cursor(_app) do
        GenServer.call(__MODULE__, {:state, :cursor}, 5000)
    end

    @impl true
    def publish(staged, _opts \\ [])
    def publish(staged, _opts) when is_list(staged) do
        case GenServer.call(Repo, {:publish, staged}, 5000) do
            {:ok, events} ->
                Enum.map(events, fn event -> 
                    GenServer.cast(Broker, {:broadcast, event})
                end)
                :ok

            error ->
                error
        end
    end

    def publish(staged, opts) do
        List.wrap(staged)
        |> publish(opts)
    end

    @impl true
    def subscribe(opts \\ [])

    def subscribe(opts) when is_list(opts) do
        subscribe(nil, [])
    end

    def subscribe(handle) when is_binary(handle) do
        subscribe(handle, [])
    end

    @impl true
    def subscribe(nil, opts) when is_list(opts) do
        GenServer.call(Broker, {:subscribe, opts}, 5000)
    end

    @impl true
    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    @impl true
    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        opts = [handle: handle] ++ opts
        GenServer.call(Broker, {:subscribe, opts}, 5000)
    end

    @impl true
    def unsubscribe(_opts \\ []) do
        GenServer.call(Broker, :unsubscribe, 5000)
    end

    @impl true
    def subscription(_opts \\ []) do
        GenServer.call(Broker, :subscription, 5000)
    end

    @impl true
    def acknowledge(number, _opts \\ []) do
        GenServer.cast(Broker, {:ack, self(), number})
    end

    @impl true
    def record(%Snapshot{}=snapshot, _opts) do
        GenServer.call(Repo, {:record, snapshot}, 500)
    end

    @impl true
    def snapshot(iden, opts) do
        GenServer.call(Repo, {:snapshot, iden, opts}, 500)
    end

    @impl true
    def stream_position(stream, _opts \\ []) when is_tuple(stream) do
        %{position: position} =
            GenServer.call(Repo, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream) == stream))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{position: 0} end)
        position
    end

end
