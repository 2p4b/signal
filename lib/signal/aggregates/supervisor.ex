defmodule Signal.Aggregates.Supervisor do
    use DynamicSupervisor
    use Signal.Superviser, registry: Aggregate

    def start_link(args) do
        DynamicSupervisor.start_link(__MODULE__, args, name: name(args) )
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Aggregates.Aggregate, args})
    end

    def prepare_aggregate(application, {type, id}) 
    when is_binary(id) and is_atom(type) do
        case Registry.lookup(registry(application), id) do
            [{_pid, type}] ->
                via_tuple(application, {id, type})            

            [] ->
                via_name = via_tuple(application, {id, type})
                application
                |> child_args(via_name) 
                |> start_child()
                prepare_aggregate(application, {type, id})
        end
    end

    defp child_args(app, via_name) when is_tuple(via_name) do
        {_, _, {_registry, stream, type}} = via_name
        {app_module, _app_name} = app
        [
            name: via_name,
            state: struct!(type, []),
            stream: {type, stream},
            store: Kernel.apply(app_module, :store, []),
            app: app,
        ] 
    end

end
