defmodule Signal.Events.ProducerTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store
    alias Signal.Stream.History
    alias Signal.Stream.Producer

    defmodule TestApp do
        use Signal.Application,
            store: Store
    end

    defmodule Aggregate do
        use Signal.Aggregate

        schema do
            field :id,      :string,   default: ""
            field :uuid,    :string,   default: ""
        end

        def apply(%Aggregate{}=aggr, _event) do
            aggr
        end
    end

    defmodule EventOne do
        use Signal.Event,
            stream: {Aggregate, :uuid}

        schema do
            field :id,      :string,   default: "event.id"
            field :uuid,    :string,   default: "stream.one"
        end
    end

    defmodule EventTwo do
        use Signal.Event,
            stream: {Aggregate, :uuid}

        schema do
            field :id,      :string,   default: "event.id"
            field :uuid,    :string,   default: "stream.two"
        end
    end

    defmodule EventThree do
        use Signal.Event,
            stream: {Aggregate, :uuid}

        schema do
            field :id,      :string,   default: "event.id"
            field :uuid,    :string,   default: "stream.two"
        end
    end

    defmodule Command do

        use Signal.Command,
            stream: {Aggregate, :uuid}

        schema do
            field :id,      :string,   default: "command.id"
            field :uuid,    :string,   default: "command.uuid"
        end

        def handle(_command, _params, _aggregate) do
            [
                EventOne.new(),
                EventTwo.new(),
                EventThree.new()
            ]
        end

        def execute(%Command{uuid: uuid}, _params) do
            {:ok, uuid}
        end
    end

    setup_all do
        start_supervised(Store)
        {:ok, _pid} = start_supervised(TestApp)
        :ok
    end

    describe "Producer" do
        @tag :producer
        test "should handle command" do
            action =
                Command.new([])
                |> Signal.Task.new([app: TestApp])
                |> Signal.Command.Action.from()

            {:ok, histories}  = Producer.process(action)
            size = length(histories)
            assert size == 2

            [%History{}=first, %History{}=second] = histories

            assert match?(%History{
                stream: {"stream.one", Aggregate},
                version: 1,
            },  first)

            assert length(first.events) == 1

            #stream.two events count
            assert Kernel.hd(first.events) |> Map.get(:topic) == Signal.Helper.module_to_string(EventOne)

            assert match?(%History{
                stream: {"stream.two", Aggregate},
                version: 2,
            }, second)

            assert length(second.events) == 2
            #stream.two events count
            assert Kernel.hd(second.events) |> Map.get(:topic) == Signal.Helper.module_to_string(EventTwo)
        end

    end

end
