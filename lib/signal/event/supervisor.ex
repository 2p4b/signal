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

    def prepare_broker(app, id, opts \\ []) when is_binary(id) do
        case Registry.lookup(registry(app), id) do
            [{_pid, _type}] ->
                via_tuple(app, {id, []})            

            [] ->
                via_name =
                    app
                    |> via_tuple({id, []})

                app
                |> child_args(id, via_name)
                |> start_child()

                prepare_broker(app, id, opts)
        end
    end

    defp child_args(app, id, via_name) do
        [
            handle: id,
            app: app,
            name: via_name
        ] 
    end

    def broker(app, id)  when is_binary(id) do
        case Registry.lookup(registry(app), id) do
            [{_pid, _type}] ->
                via_tuple(app, {id, []})

            [] ->
                nil
        end
    end

end
