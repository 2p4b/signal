defmodule Signal.Execution.Supervisor do
    use DynamicSupervisor
    use Signal.Superviser, registry: Queue

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg))
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Execution.Queue, args})
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def prepare_queue(app, queue, type, opts \\ [])
    when is_binary(queue) and is_atom(type) do
        case Registry.lookup(registry(app), queue) do
            [{_pid, type}] ->
                via_tuple(app, {queue, type})            

            [] ->
                via_name = via_tuple(app, {queue, type})
                app
                |> queue_args(via_name, opts)
                |> start_child()
                prepare_queue(app, queue, type, opts)
        end
    end

    defp queue_args(app, name, opts) when is_list(opts) do
        {_, _, {_registry, id, type}} = name

        [ app: app, id: id, name: name, type: type ]
        ++
        Kernel.apply(app, :queue, [type]) 
        ++ 
        opts
    end

end


