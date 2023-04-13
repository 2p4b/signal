defmodule Signal.Application do

    defmacro __using__(opts) do
        store = Keyword.get(opts, :store)
        quote generated: true, location: :keep do
            use Supervisor

            import unquote(__MODULE__)
            import Signal.Router, only: [via: 1, pipe: 2, pipeline: 2]

            @store unquote(store)

            @before_compile unquote(__MODULE__)
            Module.register_attribute(__MODULE__, :queues, accumulate: true)
            Module.register_attribute(__MODULE__, :signal_routers, accumulate: true)

            def start_link(init_arg) do
                Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
            end

            def init(init_arg) do
                default_args = [app: __MODULE__, store: @store]

                bus = Signal.PubSub.event_bus(__MODULE__)

                children = [
                    { Phoenix.PubSub, name: bus},
                    { Signal.Tracker, default_args},
                    { Signal.Store.Writer, default_args},
                    { Task.Supervisor, supervisor_args(Task)},
                    { Signal.Registry.Supervisor, default_args},
                    { Signal.Aggregates.Supervisor, default_args },
                    { Signal.Execution.Supervisor, default_args },
                    { Signal.Process.Supervisor, default_args },
                    { Signal.Event.Supervisor, default_args },
                    { Signal.Stream.Supervisor, default_args },
                ]
                opts = [strategy: :one_for_one, name: __MODULE__]
                Supervisor.init(children, opts)
            end

            def subscribe(opts\\[]) do
                opts = Keyword.merge(opts, cast: true)
                __MODULE__
                |> Signal.PubSub.subscribe_to_events(opts)
            end

            def unsubscribe() do
                __MODULE__
                |> Signal.PubSub.unsubscribe_from_events()
            end

            def store(), do: @store

            def get_event(number, _opts \\ []) do
                __MODULE__
                |> Signal.Store.Adapter.get_event(number)
            end

            def commit(transaction, opts \\ []) do
                Signal.Store.Writer.commit(__MODULE__, transaction, opts)
            end

            defdelegate stream_position(stream, opts\\[]), to: @store

            defdelegate get_snapshot(iden, opts\\[]), to: @store

            def process_alive?(process, id, _opts\\[]) 
            when is_atom(process) and is_binary(id) do
                GenServer.call(process, {:alive, id}, 5000)
            end

            def aggregate(stream, opts\\[]) when is_tuple(stream) and is_list(opts) do
                opts = Keyword.put_new_lazy(opts, :version, fn -> 
                    stream_position(elem(stream, 0), [])
                end)
                __MODULE__
                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
                |> Signal.Aggregates.Aggregate.state(opts)
            end

            def revise_aggregate(stream, {version, state}, opts\\[]) do
                __MODULE__
                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
                |> Signal.Aggregates.Aggregate.revise({version, state}, opts)
            end

           defp supervisor_args(type) do
               [name: Signal.Application.supervisor(type, __MODULE__)]
           end

        end
    end

    def handle_from_pid(pid) when is_pid(pid) do
        :crypto.hash(:md5 , inspect(pid)) 
        |> Base.encode16()
    end

    def supervisor(superv, app) do
        Module.concat([__MODULE__, app, superv, Supervisor])
    end

    def registry(reg, app) do
        Module.concat([__MODULE__, app, reg, Registry])
    end

    def name(name, app) do
        Module.concat([__MODULE__, app, name])
    end

   #def registry_application(registry) when is_atom(registry) do
   #    registry
   #    |> Module.split()
   #    |> Enum.drop(-2)
   #    |> Module.concat()
   #end

    defmacro router(router_module, opts \\ []) do
        quote do
            Module.put_attribute(__MODULE__,:signal_routers, {unquote(router_module), build_pipeline(__MODULE__, unquote(opts))})
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

    def build_pipeline(app, opts) do 
        Keyword.update(opts, :via, [], fn pipeline -> 
            cond do
                is_atom(pipeline) ->
                    [{app, pipeline}]

                is_list(pipeline) ->
                    Enum.map(pipeline, &({app, &1}))

                true ->
                    []
            end
        end)
    end

    defmacro __before_compile__(_env) do

        quote generated: true do


            def handler(command) when is_struct(command) do
                error_value = {:error, :unroutable, command}

                Enum.find_value(@signal_routers, error_value, fn {router, ropts} -> 
                    if Kernel.apply(router, :dispatchable?, [command]) do
                        {router, ropts}
                    end
                end)
            end

            def dispatch(command, opts \\ [])
            def dispatch(command, opts) when is_struct(command) and is_list(opts) do
                case handler(command) do
                    {router, r_opts} ->
                        opts = 
                            r_opts
                            |> Keyword.merge(opts)
                            |> Keyword.put(:app, __MODULE__)
                        Kernel.apply(router, :dispatch, [command, opts])

                    error ->
                        error
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
