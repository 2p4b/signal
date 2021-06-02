defmodule Signal.Events.EventTest do
    use ExUnit.Case, async: true

    alias Signal.Stream

    defmodule Event do
        use Signal.Event,
            version: "v1",
            stream: {Signal.Sample.Aggregate, :uuid}

        schema do
            field :id,      String.t,   default: "event.id"
            field :uuid,    String.t,   default: "event.uuid"
        end

    end

    describe "Event" do

        @tag :event
        test "can construct event struct" do
            id = "event.id.id"
            uuid = "event.id.uuid"
            event = Event.new([id: id, uuid: uuid])
            assert event.id == id
            assert event.uuid == uuid
        end

        @tag :event
        test "event has aggregate" do
            event = Event.new([])
            aggregate = Stream.stream(event)
            assert is_tuple(aggregate)
            assert aggregate |> elem(0) == Signal.Sample.Aggregate
            assert aggregate |> elem(1) == "event.uuid"
        end

    end

end

