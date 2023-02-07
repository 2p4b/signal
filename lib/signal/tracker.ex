defmodule Signal.Tracker do
    use Phoenix.Tracker

    def start_link(opts) do
        app = Keyword.fetch!(opts, :app)

        name = Module.concat(app, __MODULE__)

        bus = Signal.PubSub.event_bus(app)

        node = Phoenix.PubSub.node_name(bus)

        opts = 
            opts
            |> Keyword.merge(name: name, pubsub_server: bus, node_name: node)

        Phoenix.Tracker.start_link(__MODULE__, opts, opts)
    end

    def init(opts) do
        app =  Keyword.fetch!(opts, :app)
        node_name =  Keyword.fetch!(opts, :node_name)
        pubsub_server =  Keyword.fetch!(opts, :pubsub_server)
        {:ok, %{app: app, pubsub_server: pubsub_server, node_name: node_name}}
    end

    def handle_diff(diff, state) do
        for {topic, {joins, leaves}} <- diff do
            for {_key, meta} <- joins do
                case topic do
                    "broker" ->
                        nil

                    "consumer" ->
                        meta.app
                        |> Signal.Event.Supervisor.prepare_broker(meta.handle, [])
                        #|> GenServer.cast({:subscribe, meta})
                end
            end
            for {_key, meta} <- leaves do
                case topic do
                    "broker" ->
                        nil

                    "consumer" ->
                        broker =
                            meta.app
                            |> Signal.Event.Supervisor.broker(meta.handle)

                        if broker do
                            broker
                            |> GenServer.cast({:unsubscribe, meta.uuid})
                        end
                end
            end
        end
        {:ok, state}
    end

    def get_by_id(app, handle, id) do
        app
        |> Module.concat(__MODULE__)
        |> Phoenix.Tracker.get_by_key(handle, id)
    end

    def track(app, handle, id, data\\%{}) when is_binary(id) and is_map(data) do 
        data = Map.put(data, :node, Node.self())
        app
        |> Module.concat(__MODULE__)
        |> Phoenix.Tracker.track(self(), handle, id, data)
    end

    def untrack(app, handle, id) do
        app
        |> Module.concat(__MODULE__)
        |> Phoenix.Tracker.untrack(self(), handle, id)
    end

    def update(app, pid, handle, id, data) do
        app
        |> Module.concat(__MODULE__)
        |> Phoenix.Tracker.update(pid, handle, id, data)
    end

    def list(app, handle) do
        app
        |> Module.concat(__MODULE__)
        |> Phoenix.Tracker.list(handle)
    end

end
