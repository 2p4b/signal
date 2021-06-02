defmodule Signal.Events.ProducerTest do
    use ExUnit.Case, async: true

    alias Signal.VoidStore
    alias Signal.Stream.History
    alias Signal.Events.Producer

    defmodule TestApp do
        use Signal.Application,
            store: VoidStore
    end

    defmodule EventOne do
        use Signal.Event,
            stream: {Signal.Sample.Aggregate, :uuid}

        schema do
            field :id,      String.t,   default: "event.id"
            field :uuid,    String.t,   default: "stream.one"
        end
    end

    defmodule EventTwo do
        use Signal.Event,
            stream: {Signal.Sample.Aggregate, :uuid}

        schema do
            field :id,      String.t,   default: "event.id"
            field :uuid,    String.t,   default: "stream.two"
        end
    end

    defmodule EventThree do
        use Signal.Event,
            stream: {Signal.Sample.Aggregate, :uuid}

        schema do
            field :id,      String.t,   default: "event.id"
            field :uuid,    String.t,   default: "stream.two"
        end
    end

    defmodule Command do

        use Signal.Command, 
            stream: {Signal.Sample.Aggregate, :uuid}

        schema do
            field :id,      String.t,   default: "command.id"
            field :uuid,    String.t,   default: "command.uuid"
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
        start_supervised(VoidStore)
        {:ok, _pid} = start_supervised(TestApp)
        :ok
    end

    describe "Producer" do
        @tag :producer
        test "should handle command" do
            action =
                Command.new([])
                |> Signal.Execution.Task.new([app: {TestApp, TestApp}])
                |> Signal.Command.Action.from()

            {:ok, histories}  = Producer.process(action)
            size = length(histories)
            assert size == 2

            [%History{}=first, %History{}=second] = histories

            assert match?(%History{
                stream: {Signal.Sample.Aggregate, "stream.one"},
                version: 1,
            },  first)

            assert length(first.events) == 1

            #stream.two events count
            assert Kernel.hd(first.events) |> Map.get(:stream) == {Signal.Sample.Aggregate, "stream.one"}

            assert match?(%History{
                stream: {Signal.Sample.Aggregate, "stream.two"},
                version: 2,
            }, second)

            assert length(second.events) == 2
            #stream.two events count
            assert Kernel.hd(second.events) |> Map.get(:stream) == {Signal.Sample.Aggregate, "stream.two"}
        end

    end

end
