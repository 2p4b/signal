defmodule Signal.Helper do

    def stream_id(id, opts \\ [])
    def stream_id(id, []) when is_binary(id) do
        id
    end
    def stream_id(id, [context: _]) when is_binary(id) do
        id
    end
    def stream_id(id, opts) when is_binary(id) do
        name = Keyword.fetch!(opts, :name)

        hash_func = Keyword.get(opts, :hash, :md5)

        namespace = 
            case Keyword.get(opts, :type, :uuid) do
                :uuid ->
                    id

                ns when ns in [:dns, :url, :oid, :x500, nil] ->
                    namespaced_uuid(ns, id, hash_func)
            end

        namespaced_uuid(namespace, name, hash_func)
    end

    def namespaced_uuid(namespace, name, :md5) do
        UUID.uuid3(namespace, name)
    end

    def namespaced_uuid(namespace, name, :sha) do
        UUID.uuid5(namespace, name)
    end

    def module_to_string(module) when is_atom(module) do
        atom_to_string(module)
    end

    def atom_to_string(value) when is_atom(value) do
        case Atom.to_string(value) do
            "Elixir."<> name -> name
            name -> name
        end
    end

    def string_to_module(string) when is_binary(string) do
        string
        |> String.split(".")
        |> Module.concat()
    end

end
