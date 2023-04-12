defmodule Signal.Registry.Supervisor do
    use Supervisor

    def start_link(opts) do
        app = Keyword.fetch!(opts, :app)
        name = Module.concat(__MODULE__, app) 
        Supervisor.start_link(__MODULE__, opts, name: name)
    end

    def init(args) do
        app = Keyword.get(args, :app)
        children = [
            { Registry, registry_args(Aggregate, app) },
            { Registry, registry_args(Producer, app) },
            { Registry, registry_args(Broker, app) },
            { Registry, registry_args(Queue, app) },
            { Registry, registry_args(Saga, app) },
        ]
        Supervisor.init(children, strategy: :one_for_one)
    end

    defp registry_args(type, app) do
        [keys: :unique, name: Signal.Application.registry(type, app)]
    end

end
