defmodule Signal.Logger do
    require Logger

    @levels [:debug, :info, :notice, :warning, :error, :critical, :alert, :emergency]
    @contexts [:broker, :saga, :aggregate, :router, :handler, :queue, :producer]
    @base_logger [log: @contexts]

    def info(data, opts \\ []) do
        print(data, :info, opts)
    end

    def notice(data, opts \\ []) do
        print(data, :notice, opts)
    end

    def debug(data, opts \\ []) do
        print(data, :debug, opts)
    end

    def warning(data, opts \\ []) do
        print(data, :warning, opts)
    end

    def error(data, opts \\ []) do
        print(data, :error, opts)
    end

    def critical(data, opts \\ []) do
        print(data, :critical, opts)
    end

    def alert(data, opts \\ []) do
        print(data, :alert, opts)
    end

    def emergency(data, opts \\ []) do
        print(data, :emergency, opts)
    end

    def print(data, level \\ :info, opts \\ []) 
    def print(data, level, opts) when is_map(data) do
        data
        |> Map.to_list()
        |> print(level, opts)
    end
    def print(data, level, opts) when is_binary(data) do
        print([data], level, opts)
    end
    def print(data, level, opts)
    when is_list(data) and is_list(opts) and is_atom(level) do
        app = Keyword.get(data, :app)
        context = Keyword.get(opts, :label)
        text = dump_data(data) |> add_label(context)
        log(text, context, app, level)
    end

    def dump_data(data) do
        data
        |> Enum.reduce([], fn 

            value, acc when is_binary(value) -> 
                Enum.concat(acc, [value])

            {key, value}, acc -> 
                fname = String.Chars.to_string(key) 
                key_value =
                    [fname, ": ", inspect(value)]
                    |> Enum.join()
                    |> List.wrap()
                Enum.concat(acc, key_value)

        end)
        |> Enum.concat([""])
        |> Enum.join("\n")
    end

    def add_label(text, nil) do
        text
    end

    def add_label(text, label) when is_atom(label) do
        name = 
            label
            |> String.Chars.to_string() 
            |> String.upcase()

        ["[", name, "]", "\n", text]
        |> Enum.join()
    end

    def log(data, context, app, level \\ :info) 
    when level in @levels and is_atom(context) and is_atom(app) and not(is_nil(app)) and not(is_nil(context)) do
        loggers =
            :signal
            |> Application.get_env(app, @base_logger)
            |> Keyword.get(:log, @contexts)

        if is_atom(context) and is_list(loggers) do
            if Enum.member?(loggers, context) do
                Logger.log(level, data)
            end
        end
    end

end
