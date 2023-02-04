defmodule Signal.Event.Dispatcher do

    alias Signal.Event
    alias Phoenix.PubSub
    alias Signal.Event.Dispatcher

    def event_bus(application) do
        Module.concat(application, Signal.Event.Bus)
    end

    def dispatch(subscriptions, _writer, message) do
        #IO.inspect([subscriptions, message], label: __MODULE__)
        Enum.each(subscriptions, &(send(elem(&1, 0), message)))
    end

    def subscribe_to_events(application, opts\\[]) do
        application
        |> event_bus()
        |> PubSub.subscribe("event", [metadata: opts])
    end

    def unsubscribe_from_events(application) do
        application
        |> event_bus()
        |> PubSub.unsubscribe("event")
    end

    def broadcast_event(application, %Event{}=event) do
        application
        |> event_bus()
        |> PubSub.broadcast!("event", event, Dispatcher)
    end

end
