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
        pname = process_name(stream)
        case Registry.lookup(registry(application), pname) do
            [{_pid, _type}] ->
                via_tuple(application, {pname, stream})            

            [] ->
                via_name = via_tuple(application, {pname, stream})
                application
                |> child_args(via_name) 
                |> start_child()
                prepare_aggregate(application, stream)
        end
    end

    def process_name({type, id}) when is_atom(type) and is_binary(id) do
        id
    end

    defp child_args(app, via_name) do
        {:via, _reg, {_mreg, _pname, stream}} = via_name
        {app_module, _app_name} = app
        {aggregate, id} = stream
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
