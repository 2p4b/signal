defmodule Signal.Router do

    alias Signal.Task
    alias Signal.Command.Dispatcher

    defmacro __using__(_) do
        quote do
            import unquote(__MODULE__)
            @before_compile unquote(__MODULE__)
            Module.register_attribute(__MODULE__, :commands, accumulate: true)
            Module.register_attribute(__MODULE__, :registered_commands, accumulate: true)
            Module.register_attribute(__MODULE__, :in_pipeline, accumulate: false)
        end
    end

    defmacro register(command_module, opts \\ []) do
        quote location: :keep do
            if is_list(unquote(command_module)) do
                Enum.each(unquote(command_module), fn module -> 
                    Module.put_attribute(__MODULE__, :commands, module)
                    Module.put_attribute(__MODULE__, :registered_commands, {module, unquote(opts)})
                end)
            else
                Module.put_attribute(__MODULE__, :commands, unquote(command_module))
                Module.put_attribute(__MODULE__, :registered_commands, {unquote(command_module), unquote(opts)})
            end
        end
    end

    defmacro via(pipe_name) do
        quote location: :keep do
            Module.put_attribute(__MODULE__, :pipeline_pipes, unquote(pipe_name))
        end
    end

    defmacro pipe(name, middleware) do
        quote location: :keep do
            case unquote(middleware) do
                middleware when is_atom(middleware) ->
                    def unquote(name)(command) do
                        Kernel.apply(unquote(middleware), :handle, [command])
                    end

                middleware when is_function(middleware) ->
                    def unquote(name)(command) do
                        Kernel.apply(unquote(middleware), [command])
                    end
            end
        end
    end

    defmacro pipeline(name, do: do_block) do
        quote location: :keep do

            Module.put_attribute(__MODULE__, :in_pipeline, true)
            Module.register_attribute(__MODULE__, :pipeline_pipes, accumulate: true)

            (fn -> 
                unquote(do_block)
            end).()

            @pipeline Enum.reverse(@pipeline_pipes)

            def unquote(name)(command) do
                Enum.reduce(@pipeline, {:ok, command}, fn pipe, acc -> 
                    with {:ok, command} <- acc do
                        Kernel.apply(__MODULE__, pipe, [command])
                    else
                        error -> error
                    end
                end)
            end

            Module.delete_attribute(__MODULE__, :pipeline)
            Module.delete_attribute(__MODULE__, :pipeline_pipes)
            Module.put_attribute(__MODULE__, :in_pipeline, false)
        end
    end

    defmacro __before_compile__(_env) do
        quote generated: true do

            def dispatch(command, opts) when is_struct(command) and is_list(opts) do
                with {:ok, %Task{}=task} <- process(command, opts) do
                    Dispatcher.dispatch(task)
                else
                    error -> error
                end
            end

            def process(command, opts \\ [])
            def process(command, opts), do: run(command, opts)

            for {command_module, command_opts} <- @registered_commands do

                {pipethrough, command_opts} = Keyword.pop(command_opts, :via)

                @pipethrough pipethrough

                @command_module command_module
                @command_opts command_opts ++ [
                    queue: Keyword.get(command_opts, :queue, :default),
                    timeout: Keyword.get(command_opts, :timeout, 5000),
                ]

                @task_opts [
                    command_type: to_string(@command_module),
                ]

                if @pipethrough do
                    @pipethrough if is_atom(pipethrough), do: [pipethrough], else: pipethrough
                    @pipethrough Enum.map(@pipethrough, &({__MODULE__, &1}))

                    def run(%@command_module{} = command, opts) do
                        run(command, @pipethrough, opts)
                    end
                else
                    def run(%@command_module{} = command, opts) do
                        run(command, [], opts)
                    end
                end

                def run(%@command_module{} = command, pipeline, opts) do

                    {app_pipeline, opts} = Keyword.pop(opts, :via, [])

                    opts = Keyword.merge(@command_opts, opts)

                    task = Task.new(command, @task_opts ++ opts)

                    app_pipeline ++ pipeline
                    |> Enum.reduce({:ok, task}, fn {pipemod, pipe}, acc -> 
                        with {:ok, %Task{}=task} <- acc do
                            Kernel.apply(pipemod, pipe, [task])
                        else
                            error -> error
                        end
                    end)

                end

                def dispatchable?(%@command_module{}), do: true
            end

            def dispatchable?(_cmd), do: false

            def run(%{__struct__: type}, _opts) do
                raise ArgumentError, message: """

                    Unknown router command type #{inspect(type)}

                    Enusure that the command is registerd
                    on your router like:

                    defmodule #{inspect(__MODULE__)} do
                        use Signal.Router

                        dispatch #{inspect(type)}, [..]

                    end
                """
            end

        end
    end

end
