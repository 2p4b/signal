defmodule Signal.Store.Adapter do

    def application_store(app) when is_atom(app) do
        Kernel.apply(app, :store, [])
    end

    def get_effect(app, uuid, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :get_effect, [uuid, opts])
    end

    def save_effect(app, effect, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :save_effect, [effect, opts])
    end

    def list_effects(app, namespace, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :list_effects, [namespace, opts])
    end

    def delete_effect(app, uuid, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :delete_effect, [uuid, opts])
    end

    def commit_transaction(app, transaction, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :commit_transaction, [transaction, opts])
    end

    def get_cursor(app, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :get_cursor, [opts])
    end

    def get_event(app, number, opts\\[]) do 
        opts = Keyword.merge(opts, [range: [number, number]])
        case list_events(app, opts) do
            [head| _] ->
                head

            [] ->
                nil

            unknown -> 
                # Should not reach here!
                unknown
        end
    end

    def get_stream_event(app, stream_id, version, opts\\[]) do
        opts = Keyword.merge(opts, [range: [version, version]])
        case list_stream_events(app, stream_id, opts) do
            [head| _] ->
                head

            [] ->
                nil

            unknown -> 
                # Should not reach here!
                unknown
        end
    end

    def read_events(app, callback, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :read_events, [callback, opts])
    end

    def read_stream_events(app, stream_id, callback, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :read_stream_events, [stream_id, callback, opts])
    end

    def list_events(app, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :list_events, [opts])
    end

    def list_stream_events(app, stream_id, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :list_stream_events, [stream_id, opts])
    end

    def handler_position(app, handle, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :handler_position, [handle, opts])
    end

    def handler_acknowledge(app, handle, number, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :handler_acknowledge, [handle, number, opts])
    end

    def get_snapshot(app, id, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :get_snapshot, [id, opts])
    end

    def delete_snapshot(app, id, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :delete_snapshot, [id, opts])
    end

    def record_snapshot(app, snapshot, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :record_snapshot, [snapshot, opts])
    end

    def stream_position(app, stream, opts\\[]) do
        store = application_store(app)
        Kernel.apply(store, :stream_position, [stream, opts])
    end

end

