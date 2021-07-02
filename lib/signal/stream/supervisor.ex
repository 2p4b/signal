defmodule Signal.Stream.Supervisor do

    use DynamicSupervisor
    use Signal.Superviser, registry: Broker

    alias Signal.Helper

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg))
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Stream.Broker, args})
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def prepare_broker(app, name) when is_tuple(app) and is_atom(name) do
        prepare_broker(app, Helper.module_to_string(name))
    end

    def prepare_broker(app, name) when is_binary(name) do
        case Registry.lookup(registry(app), name) do
            [{_pid, _name}] ->
                via_tuple(app, {name, nil})            

            [] ->
                via_name =  via_tuple(app, {name, nil})

                app
                |> broker_args(via_name)
                |> start_child()

                prepare_broker(app, name)
        end
    end

    defp broker_args(app, {_, _, {_, type, _app}}=name) do
        [name: name, app: app, store: Signal.Application.store(app), type: Helper.string_to_module(type)]
    end

end

