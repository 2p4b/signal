defmodule Signal.Void.Store do
    use Supervisor

    alias Signal.Snapshot
    alias Signal.Void.Repo
    alias Signal.Void.Broker
    require Logger

    @behaviour Signal.Store

    def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
    end

    @impl true
    def init(_init_arg) do
        children = [Repo, Broker]
        opts = [strategy: :one_for_one, name: __MODULE__]
        Supervisor.init(children, opts)
    end

    @impl true
    def index(_app) do
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
    def purge(snap, opts \\ []) do
        Repo.purge(snap, opts)
    end

    @impl true
    def event(number, _opts \\ []) do
        Repo.event(number)
    end

    @impl true
    def subscribe(handle, opts) when is_list(opts) and is_atom(handle) do
        subscribe(Atom.to_string(handle), opts)
    end

    @impl true
    def subscribe(handle, opts) when is_list(opts) and is_binary(handle) do
        Broker.subscribe(handle, opts)
    end

    @impl true
    def unsubscribe(handle, opts \\ []) do
        Broker.unsubscribe(handle, opts)
    end

    @impl true
    def subscription(handle, _opts \\ []) do
        Broker.subscription(handle)
    end

    @impl true
    def acknowledge(handle, number, _opts \\ []) do
        Broker.acknowledge(handle, number)
    end

    @impl true
    def record(%Snapshot{}=snapshot, opts) do
        Repo.record(snapshot, opts)
    end

    @impl true
    def snapshot(iden, opts) do
        Repo.snapshot(iden, opts)
    end

    @impl true
    def stream_position(stream, _opts \\ []) when is_binary(stream) do
        %{position: position} =
            GenServer.call(Repo, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream) == stream))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{position: 0} end)
        position
    end

end
