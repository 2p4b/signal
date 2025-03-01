defmodule Signal.Type do

    defmacro __using__(_) do
        quote do
            import Signal.Type, only: [schema: 1, schema: 2]
        end
    end

    defmacro schema(opts\\[], do: block) do
        quote do
            Signal.Type.__define__(
                unquote(Macro.escape(block)),
                unquote(opts)
            )
        end
    end

    # Most of code here is sourced from typed_struct lib
    defmacro __define__(block, opts) do
        quote do
            Module.register_attribute(__MODULE__, :ts_keys, accumulate: true)
            Module.register_attribute(__MODULE__, :ts_types, accumulate: true)
            Module.register_attribute(__MODULE__, :ts_typed, accumulate: true)
            Module.register_attribute(__MODULE__, :ts_fields, accumulate: true)
            Module.register_attribute(__MODULE__, :ts_enforce_keys, accumulate: true)
            Module.put_attribute(__MODULE__, :ts_enforce?, unquote(!!opts[:enforce]))


            # Create a scope to avoid leaks.
            (fn ->
                import Signal.Type
                Code.eval_quoted(unquote(block), [], __ENV__)
            end).()

            @enforce_keys @ts_enforce_keys
            defstruct @ts_fields


            Signal.Type.__type__(@ts_types, unquote(opts))

            Signal.Type.__contructor__(@ts_keys)

            Module.delete_attribute(__MODULE__, :ts_enforce_keys)
            Module.delete_attribute(__MODULE__, :ts_enforce?)
            Module.delete_attribute(__MODULE__, :ts_fields)
            Module.delete_attribute(__MODULE__, :ts_types)
            Module.delete_attribute(__MODULE__, :ts_keys)
        end
    end

    defmacro __contructor__(ts_keys) do
        quote do
            Module.register_attribute(__MODULE__, :ts_str_keys, accumulate: true)

            Enum.each(unquote(ts_keys), fn key -> 
                Module.put_attribute(__MODULE__, :ts_str_keys, {key, to_string(key)})
            end)

            def new(attr \\ %{})
            def new(attr) when is_map(attr) do
                params = 
                    Enum.reduce(@ts_str_keys, %{}, fn ({akey, skey}, acc) -> 
                        cond do
                            Map.has_key?(attr, akey) ->
                                Map.put(acc, akey, Map.get(attr, akey))

                            Map.has_key?(attr, skey) ->
                                Map.put(acc, akey, Map.get(attr, skey))
                            true ->
                                acc
                        end
                    end)

                struct!(__MODULE__, params)
            end

            def new(attr) when is_list(attr) do
                # Let elixir handle it!
                struct(__MODULE__, attr)
            end

            def from(strt, remap\\[]) when is_struct(strt) do
                params =
                    Enum.reduce(remap, Map.from_struct(strt), fn({nkey, okey}, acc) ->
                        with {:ok, value} <- Map.fetch(acc, okey) do
                            Map.put(acc, nkey, value)
                        else
                            _ -> acc 
                        end
                    end)
                Kernel.apply(__MODULE__, :new, [params])
            end

            Module.delete_attribute(__MODULE__, :ts_keys_str)
        end
    end

    defmacro __type__(types, opts) do
        if Keyword.get(opts, :opaque, false) do
            quote bind_quoted: [types: types] do
                @opaque t() :: %__MODULE__{unquote_splicing(types)}
            end
        else
            quote bind_quoted: [types: types] do
                @type t() :: %__MODULE__{unquote_splicing(types)}
            end
        end
    end

    defmacro field(name, type, opts \\ []) do
        quote do
            Signal.Type.__field__(
                __MODULE__,
                unquote(name),
                unquote(Macro.escape(type)),
                unquote(opts)
            )
        end
    end

    @doc false
    def __field__(mod, name, type, opts) when is_atom(name) do
        if mod |> Module.get_attribute(:ts_fields) |> Keyword.has_key?(name) do
            raise ArgumentError, "the field #{inspect(name)} is already set"
        end

        has_default? = Keyword.has_key?(opts, :default)
        enforce_by_default? = Module.get_attribute(mod, :ts_enforce?)

        enforce? =
            if is_nil(opts[:enforce]) do
                enforce_by_default? && !has_default?
            else
                !!opts[:enforce]
            end

        nullable? = !has_default? && !enforce?

        Module.put_attribute(mod, :ts_keys, name)
        Module.put_attribute(mod, :ts_fields, {name, opts[:default]})
        Module.put_attribute(mod, :ts_types, {name, type_for(type, nullable?)})

        if enforce? do 
            Module.put_attribute(mod, :ts_enforce_keys, name)
        end

    end

    def __field__(_mod, name, _type, _opts) do
        raise ArgumentError, "a field name must be an atom, got #{inspect(name)}"
    end

    # Makes the type nullable if the key is not enforced.
    defp type_for(type, false), do: type

    defp type_for(type, _), do: quote(do: unquote(type) | nil)

end

