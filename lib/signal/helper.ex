defmodule Signal.Helper do

    def stream_id(tuple, opts \\ [])
    def stream_id({nil, id}, _opts) when is_binary(id) do
        id
    end
    def stream_id({tag, id}, opts) when is_atom(tag) and is_binary(id) do
        domain = atom_to_string(tag)
        namespace = UUID.uuid3(:oid, domain)
        hash_func = Keyword.get(opts, :hash, :sha)
        namespaced_uuid(namespace, id, hash_func)
    end

    def namespaced_uuid(namespace, id, :sha) do
        UUID.uuid5(namespace, id)
    end

    def namespaced_uuid(namespace, id, :md5) do
        UUID.uuid3(namespace, id)
    end

    def module_to_string(atom) when is_atom(atom) do
        atom_to_string(atom)
    end

    def atom_to_string(atom) when is_atom(atom) do
        case Atom.to_string(atom) do
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
