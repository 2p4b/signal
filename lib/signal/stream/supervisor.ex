defmodule Signal.Stream.Supervisor do
    use DynamicSupervisor
    use Signal.Superviser, registry: Producer

    def start_link(init_arg) do
        DynamicSupervisor.start_link(__MODULE__, init_arg, name: name(init_arg) )
    end

    def start_child(args) when is_list(args) do
        DynamicSupervisor.start_child(name(args), {Signal.Stream.Producer, args})
    end

    @impl true
    def init(_init_arg) do
        DynamicSupervisor.init(strategy: :one_for_one)
    end

    def prepare_producer(application, {id, type}=stream) 
    when is_atom(type) and is_binary(id) do
        case Registry.lookup(registry(application), id) do
            [{_pid, _id}] ->
                via_tuple(application, {id, stream})            

            [] ->
                via_name = via_tuple(application, {id, stream})

                application
                |> producer_args(stream, via_name)
                |> start_child()

                prepare_producer(application, stream)
        end
    end

    defp producer_args(app, stream, via_name) do
        [
            app: app,
            name: via_name,
            stream: stream, 
        ]
    end

end


