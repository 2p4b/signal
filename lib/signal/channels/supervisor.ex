defmodule Signal.Channels.Supervisor do

    use DynamicSupervisor
    use Signal.Superviser, registry: Channel

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg))
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Channels.Channel, args})
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def prepare_channel(app, name) when is_binary(name) do
        case Registry.lookup(registry(app), name) do
            [{_pid, _name}] ->
                via_tuple(app, {name, nil})            

            [] ->
                via_name = via_tuple(app, {name, nil})
                app
                |> channel_args(via_name)
                |> start_child()
                prepare_channel(app, name)
        end
    end

    defp channel_args(app, {_, _, {_, id, _}}=name) when is_tuple(name) do
        {app_module, _app_name} = app
        [ name: name, id: id, app: app, store: app_module.store() ]
    end

end

