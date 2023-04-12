defmodule Signal.Superviser do
    defmacro __using__(opts) do
        registry = Keyword.get(opts, :registry)
        quote do

            @registry unquote(registry)

            defp name(module) when is_atom(module) do
                Module.concat([__MODULE__, module])
            end

            defp name(args) when is_list(args) do
                args |> Keyword.get(:app) |> name()
            end

            if @registry do

                defp via_tuple(application, {id, value}) when is_binary(id) do
                    {:via, Registry, {registry(application), id, value}}
                end

                defp registry(app) do
                    Signal.Application.registry(@registry, app)
                end

                def unregister_child(app, id) do
                    Registry.unregister(registry(app), id)
                end

                def stop_child(app, id) when is_binary(id) do
                    case Registry.lookup(registry(app), id) do
                        [{pid, _name}] ->
                            unregister_child(app, id)
                            DynamicSupervisor.terminate_child(name(app), pid)

                        [] -> {:error, :not_found}
                    end
                end

            end

        end
    end
end
