defmodule Signal.Sample.Void do
    def get_cursor do
        0
    end

    def get_stream_index(_stream) do
    end

    def commit(_transaction, _opts) do
        :ok
    end
end
