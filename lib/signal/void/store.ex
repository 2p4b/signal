defmodule Signal.Void.Store do
    use Supervisor

    alias Signal.Snapshot
    alias Signal.Void.Repo
    alias Signal.Transaction

    @behaviour Signal.Store

    def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
    end

    @impl true
    def init(_init_arg) do
        children = [Repo]
        opts = [strategy: :one_for_one, name: __MODULE__]
        Supervisor.init(children, opts)
    end

    def get_event(number, _opts \\ []) do
        Repo.get_event(number)
    end

    @impl true
    def get_effect(uuid, _opts\\[]) do
        GenServer.call(Repo, {:get_effect, uuid}, 5000)
    end

    @impl true
    def save_effect(effect, _opts\\[]) do
        GenServer.call(Repo, {:save_effect, effect}, 5000)
    end

    @impl true
    def delete_effect(uuid, _opts\\[]) do
        GenServer.call(Repo, {:delete_effect, uuid}, 5000)
    end

    @impl true
    def get_cursor(_opts) do
        GenServer.call(Repo, {:state, :cursor}, 5000)
    end

    @impl true
    def commit_transaction(transaction, opts \\ [])
    def commit_transaction(%Transaction{}=transaction, _opts) do
        GenServer.call(Repo, {:commit, transaction}, 5000)
    end

    @impl true
    def handler_position(handle, _opts \\ []) do
        Repo.handler_position(handle)
    end

    @impl true
    def handler_acknowledge(handle, number, _opts \\ []) do
        Repo.handler_acknowledge(handle, number)
    end

    @impl true
    def record_snapshot(%Snapshot{}=snapshot, opts) do
        Repo.record_snapshot(snapshot, opts)
    end

    @impl true
    def delete_snapshot(id, opts \\ []) do
        Repo.delete_snapshot(id, opts)
    end

    @impl true
    def get_snapshot(id, opts) do
        Repo.get_snapshot(id, opts)
    end

    @impl true
    def read_events(reader, params, _opts) do
        Repo.read_events(reader, params)
    end

    @impl true
    def read_stream_events(sid, reader, params, _opts) do
        Repo.read_stream_events(sid, reader, params)
    end

    @impl true
    def list_events(params, _opts) do
        Repo.list_events(params)
    end

    @impl true
    def list_stream_events(sid, params, _opts) do
        Repo.list_stream_events(sid, params)
    end

    @impl true
    def stream_position(stream_id, _opts \\ []) when is_binary(stream_id) do
        %{position: position} =
            GenServer.call(Repo, {:state, :events}, 5000)
            |> Enum.filter(&(Map.get(&1, :stream_id) == stream_id))
            |> Enum.max_by(&(Map.get(&1, :number)), fn -> %{position: 0} end)
        position
    end

end
