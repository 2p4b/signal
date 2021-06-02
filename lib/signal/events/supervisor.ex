defmodule Signal.Events.Supervisor do
    use DynamicSupervisor
    use Signal.Superviser, registry: Producer

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg) )
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Events.Producer, args})
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def prepare_producer(application, {type, id}) when is_atom(type) and is_binary(id) do
        case Registry.lookup(registry(application), id) do
            [{_pid, type}] ->
                via_tuple(application, {id, type})            

            [] ->
                via_name = via_tuple(application, {id, type})

                application
                |> producer_args(via_name)
                |> start_child()
                prepare_producer(application, {type, id})
        end
    end

    defp producer_args(app, via_name) when is_tuple(via_name)  do
        {_, _, {_registry, id, type}} = via_name
        {app_module, _name} = app
        [
            app: app,
            name: via_name,
            stream: {type, id}, 
            store: Kernel.apply(app_module, :store, [])
        ]
    end

end


