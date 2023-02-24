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

    def prepare_saga(application, {id, module}=saga, opts) 
    when is_binary(id) and is_atom(module) do
        case Registry.lookup(registry(application), id) do
            [{_pid, _type}] ->
                application
                |> via_tuple({id, opts})            

            [] ->
                via_name =
                    application
                    |> via_tuple({id, opts})

                application
                |> child_args(via_name)
                |> start_child()
                prepare_saga(application, saga, opts)
        end
    end

    defp child_args(app, via_name) do
        {_, _, {_registry, uuid, opts}} = via_name
        [
            app: app,
            uuid: uuid,
            name: via_name,
        ] 
        |> Keyword.merge(opts)
    end

end

