defmodule Signal.Superviser do
    defmacro __using__(opts) do
        registry = Keyword.get(opts, :registry)
        quote do

            @registry unquote(registry)

            defp name({module, app_name}) do
                if module == app_name do
                    Module.concat([module, __MODULE__])
                else
                    Module.concat([module, __MODULE__, app_name])
                end
            end

            defp name(args) when is_list(args) do
                args |> Keyword.get(:app) |> name()
            end

            if @registry do

                defp via_tuple(application, {id, value}) when is_binary(id) do
                    {:via, Registry, {registry(application), id, value}}
                end

                defp registry({_module, _name}=app) do
                    app
                    |> Signal.Application.registry(@registry)
                end
            end

        end
    end
end
