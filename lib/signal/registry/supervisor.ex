defmodule Signal.Registry.Supervisor do
    use Supervisor

    def start_link(opts) do
        name = 
            case Keyword.get(opts, :app) do
                {module, name} when module == name ->
                    Module.concat(module, __MODULE__)
                {module, name} ->
                    Module.concat([module, __MODULE__, name])
            end
        Supervisor.start_link(__MODULE__, opts, name: name)
    end

    def init(args) do
        app = Keyword.get(args, :app)
        children = [
            { Registry, registry_args(app, Aggregate) },
            { Registry, registry_args(app, Producer) },
            { Registry, registry_args(app, Broker) },
            { Registry, registry_args(app, Queue) },
            { Registry, registry_args(app, Saga) },
        ]
        Supervisor.init(children, strategy: :one_for_one)
    end

    defp registry_args(app, type) do
        [keys: :unique, name: Signal.Application.registry(app, type)]
    end

end
