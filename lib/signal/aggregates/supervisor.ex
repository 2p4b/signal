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

    def prepare_aggregate(app, {id, type}=stream) 
    when is_binary(id) and is_atom(type) do
        pname = process_name(stream)
        case Registry.lookup(registry(app), pname) do
            [{_pid, _type}] ->
                via_tuple(app, {pname, stream})            

            [] ->
                via_name = via_tuple(app, {pname, stream})
                app
                |> child_args(via_name) 
                |> start_child()
                prepare_aggregate(app, stream)
        end
    end

    def process_name({id, _type}) when is_binary(id) do
        id
    end

    defp child_args(app, via_name) do
        {:via, _reg, {_mreg, _pname, stream}} = via_name
        {id, aggregate} = stream
        state = struct!(aggregate, [])
        config = Signal.Aggregate.Config.config(state) 
        [
            id: id,
            name: via_name,
            state: state,
            stream: stream,
            store: Kernel.apply(app, :store, []),
            app: app,
        ] 
        |> Enum.concat(config)
    end

end
