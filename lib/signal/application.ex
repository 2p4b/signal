defmodule Signal.Application do

    alias Signal.Events.Event

    defmacro __using__(opts) do
        name  = Keyword.get(opts, :name)
        store = Keyword.get(opts, :store)
        quote do
            use Supervisor

            import unquote(__MODULE__)

            @app __MODULE__

            @name (if unquote(name) do unquote(name) else __MODULE__ end)

            @store unquote(store)

            @before_compile unquote(__MODULE__)
            Module.register_attribute(__MODULE__, :queues, accumulate: true)
            Module.register_attribute(__MODULE__, :registered_routers, accumulate: true)

            def start_link(init_arg) do
                name = Keyword.get(init_arg, :name, @name)
                Supervisor.start_link(__MODULE__, init_arg, name: name)
            end

            def init(init_arg) do
                name = Keyword.get(init_arg, :name, @name)
                app = {__MODULE__, name}

                default_args = [app: app, store: @store]

                children = [
                    { Phoenix.PubSub, name: Signal.Application.bus(app)},
                    { Signal.Events.Recorder, default_args},
                    { Task.Supervisor, supervisor_args(Task, name)},
                    { Signal.Registry.Supervisor, default_args},
                    { Signal.Aggregates.Supervisor, default_args },
                    { Signal.Execution.Supervisor, default_args },
                    { Signal.Channels.Supervisor, default_args },
                    { Signal.Process.Supervisor, default_args },
                    { Signal.Events.Supervisor, default_args },
                    { Signal.Stream.Supervisor, default_args },
                ]
                opts = [strategy: :one_for_one, name: Signal.Application.name({__MODULE__, name}, Supervisor)]
                Supervisor.init(children, opts)
            end

            def store(), do: @store

            defdelegate subscribe(), to: @store

            defdelegate subscribe(name), to: @store 

            defdelegate subscribe(opts, name), to: @store

            defdelegate unsubscribe(), to: @store

            defdelegate unsubscribe(name), to: @store

            defdelegate stream_position(stream), to: @store

            defp supervisor_args(type, name) do
                [name: Signal.Application.supervisor({__MODULE__, name}, type)]
            end

            defp registry_args(type, name) do
                [keys: :unique, name: Signal.Application.registry({__MODULE__, name}, type)]
            end

        end
    end

    def store({module, _name}) do
        Kernel.apply(module, :store, [])
    end

    def supervisor({module, name}, type) do
        if module == name do
            Module.concat([module, type, Supervisor])
        else
            Module.concat([module, type, Supervisor, name])
        end
    end

    def registry({module, name}, registry) do
        if module == name do
            Module.concat([module, registry, Registry])
        else
            Module.concat([module, registry, Registry, name])
        end
    end

    def name({module, name}, value) do
        if module == name do
            Module.concat(module, value)
        else
            Module.concat([module, name, value])
        end
    end

    def bus({module, name}) when is_atom(name) do
        Module.concat([module, name, :bus])
    end

    def publish(app, %Event{}=event) when is_tuple(app) do
        Phoenix.PubSub.broadcast(bus(app), "bus", event)
    end

    def listen(app) when is_tuple(app) do
        Phoenix.PubSub.subscribe(bus(app), "bus")
    end

    def unlisten(app) when is_tuple(app) do
        Phoenix.PubSub.unsubscribe(bus(app), "bus")
    end

    def registry_application(registry) when is_atom(registry) do
        registry
        |> Module.split()
        |> Enum.drop(-2)
        |> Module.concat()
    end

    defmacro router(router_module) do
        quote do
            Module.put_attribute(__MODULE__,:registered_routers, unquote(router_module))
        end
    end

    defmacro queue([{type, opts}]) when is_atom(type) and is_list(opts) do
        __queue__(type, opts)
    end

    defmacro queue(type, opts \\ []) when is_atom(type) and is_list(opts) do
        __queue__(type, opts)
    end

    defp __queue__(type, opts) do
        quote do
            Module.put_attribute(__MODULE__, :queues, {unquote(type), unquote(opts)})
        end
    end

    defmacro __before_compile__(_env) do

        quote generated: true do
            def dispatch(command, opts \\ [])
            def dispatch(command, opts) when is_struct(command) and is_list(opts) do
                router = Enum.find(@registered_routers, fn router -> 
                    Kernel.apply(router, :dispatchable?, [command])
                end)
                if router do
                    opts =
                        case Keyword.fetch(opts, :app) do
                            {:ok, app} when is_atom(app) and not(app in [nil, :true, :false]) ->
                                app = {__MODULE__, app}
                                Keyword.merge(opts, [app: app])

                            :error ->
                                app = {__MODULE__, __MODULE__}
                                Keyword.merge(opts, [app: app])

                            _ -> 
                                opts
                        end
                    Kernel.apply(router, :dispatch, [command, opts])
                else
                    {:error, :command_not_routeable}
                end
            end

            for {queue, opts} <- @queues do
                @queue queue
                @queue_opts opts
                def queue(@queue), do: @queue_opts
            end

            def queue(queue) do
                raise ArgumentError, message: """

                    Unknown application queue type #{inspect(queue)}

                    Enusure queue type is spelled correctly and or is 
                    defined for your application like

                    defmodule #{inspect(__MODULE__)} do
                        use Signal.Application
                        queue #{inspect(queue)}, [..]
                    end
                """
            end
        end

    end
end
