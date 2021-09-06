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

    def prepare_aggregate(application, {type, id}=stream) 
    when is_binary(id) and is_atom(type) do
        case Registry.lookup(registry(application), id) do
            [{_pid, type}] ->
                via_tuple(application, {id, type})            

            [] ->
                via_name = via_tuple(application, {id, type})
                application
                |> child_args(stream, via_name) 
                |> start_child()
                prepare_aggregate(application, stream)
        end
    end

    defp child_args(app, stream, via_name) do
        {:via, _reg, {_mreg, id, _mod}} = via_name
        {aggregate, _aid} = stream
        {app_module, _app_name} = app
        [
            id: id,
            name: via_name,
            state: struct!(aggregate, []),
            stream: stream,
            store: Kernel.apply(app_module, :store, []),
            app: app,
        ] 
    end

end
