defmodule Signal.Process.Supervisor do
    use DynamicSupervisor
    use Signal.Superviser, registry: Saga

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg))
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Process.Saga, args})
    end

    def prepare_saga(application, {type, id}) 
    when is_binary(id) and is_atom(type) do
        case Registry.lookup(registry(application), id) do
            [{_pid, type}] ->
                via_tuple(application, {id, type})            

            [] ->
                via_name =
                    application
                    |> via_tuple({id, type})

                application
                |> child_args(via_name)
                |> start_child()

                prepare_saga(application, {type, id})
        end
    end

    defp child_args(app, via_name) do
        {_, _, {_registry, id, module}} = via_name
        [
            id: id,
            app: app,
            name: via_name,
            module: module,
        ] 
    end

end

