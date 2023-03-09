defmodule Signal.PubSub do
    alias Signal.Event
    alias Phoenix.PubSub

    def event_bus(application) do
        Module.concat(application, Signal.PubSub)
    end

    def dispatch(subscriptions, _writer, message) do
        #IO.inspect([subscriptions, message], label: __MODULE__)
        Enum.each(subscriptions, &(send(elem(&1, 0), message)))
    end

    def subscribe_to_events(application, opts\\[]) do
        subscribe(application, "event", [metadata: opts])
    end

    def unsubscribe_from_events(application) do
        unsubscribe(application, "event")
    end

    def broadcast_event(application, %Event{}=event) do
        broadcast(application, "event", event, Signal.PubSub)
    end

    def broadcast(application, topic, payload, dispatcher\\nil) do
        if is_nil(dispatcher) do
            application
            |> event_bus()
            |> PubSub.broadcast!(topic, payload)
        else
            application
            |> event_bus()
            |> PubSub.broadcast!(topic, payload, dispatcher)
        end
    end

    def subscribe(application, topic, opts \\ []) do
        application
        |> event_bus()
        |> PubSub.subscribe(topic, opts)
    end

    def unsubscribe(application, topic) do
        application
        |> event_bus()
        |> PubSub.unsubscribe(topic)
    end

end

