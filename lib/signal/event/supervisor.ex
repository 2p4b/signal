defmodule Signal.Event.Supervisor do

    use DynamicSupervisor
    use Signal.Superviser, registry: Broker

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg))
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Event.Broker, args})
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def prepare_broker(application, id, opts \\ []) when is_binary(id) do
        case Registry.lookup(registry(application), id) do
            [{_pid, _type}] ->
                via_tuple(application, {id, []})            

            [] ->
                via_name =
                    application
                    |> via_tuple({id, []})

                application
                |> child_args(id, via_name)
                |> start_child()

                prepare_broker(application, id, opts)
        end
    end

    defp child_args(application, id, via_name) do
        [
            handle: id,
            app: application,
            name: via_name
        ] 
    end

    def broker(application, id)  when is_binary(id) do
        case Registry.lookup(registry(application), id) do
            [{_pid, _type}] ->
                via_tuple(application, {id, []})

            [] ->
                nil
        end
    end

end
