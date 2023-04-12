defmodule Signal.PubSub do
    alias Signal.Event
    alias Phoenix.PubSub

    def event_bus(app) do
        Module.concat(app, Signal.PubSub)
    end

    def dispatch(subscriptions, _writer, message) do
        #IO.inspect([subscriptions, message], label: __MODULE__)
        Enum.each(subscriptions, &(send(elem(&1, 0), message)))
    end

    def subscribe_to_events(app, opts\\[]) do
        subscribe(app, "event", [metadata: opts])
    end

    def unsubscribe_from_events(app) do
        unsubscribe(app, "event")
    end

    def broadcast_event(app, %Event{}=event) do
        broadcast(app, "event", event, Signal.PubSub)
    end

    def broadcast(app, topic, payload, dispatcher\\nil) do
        if is_nil(dispatcher) do
            app
            |> event_bus()
            |> PubSub.broadcast!(topic, payload)
        else
            app
            |> event_bus()
            |> PubSub.broadcast!(topic, payload, dispatcher)
        end
    end

    def subscribe(app, topic, opts \\ []) do
        app
        |> event_bus()
        |> PubSub.subscribe(topic, opts)
    end

    def unsubscribe(app, topic) do
        app
        |> event_bus()
        |> PubSub.unsubscribe(topic)
    end

end

