defmodule Signal.Command.CommandTest do
    use ExUnit.Case

    alias Signal.Stream
    alias Signal.Command.Handler

    defmodule TestAggregate do
        defstruct [:uuid]
    end

    defmodule Event do

        use Signal.Event,
            stream: { TestAggregate, :uuid}

        blueprint do
            field :id,      :string,   default: "event.id"
            field :uuid,    :string,   default: "event.uuid"
        end
    end

    defmodule Command do

        use Signal.Command, 
            stream: {TestAggregate, :uuid}

        blueprint do
            field :id,      :string,   default: "command.id"
            field :uuid,    :string,   default: "command.uuid"
        end
            
        def handle(%Command{}=command, _params, _aggregate) do
            Event.from(command)
        end

        def execute(%Command{uuid: uuid}, _params) do
            {:ok, uuid}
        end

    end


    describe "commands protocol" do

        @tag :command
        test "has aggregate" do
            command = Command.new()
            aggregate = Stream.stream(command, %{})
            assert elem(aggregate, 0) == TestAggregate
            assert elem(aggregate, 1) == "command.uuid"
        end

        @tag :command
        test "handles command" do
            command = Command.new()
            event =  Handler.handle(command, %{aggregate: :aggregate}, %{})
            %Event{id: id, uuid: uuid} = event
            assert command.id == id
            assert command.uuid == uuid
        end

        @tag :command
        test "executes command" do
            assigns = %{}
            command = Command.new()
            res = Handler.execute(command, assigns)
            assert :ok == res |> elem(0)
            assert command.uuid == res |> elem(1)
        end

    end

end
