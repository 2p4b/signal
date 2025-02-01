defmodule Signal.Command.CommandTest do
    use ExUnit.Case

    alias Signal.Stream
    alias Signal.Command.Handler

    defmodule TestAggregate do
        defstruct [:uuid]
    end

    defmodule Event do
        use Signal.Event,
            stream: {:uuid, TestAggregate}

        schema do
            field :id,      :string,   default: "event.id"
            field :uuid,    :string,   default: "event.uuid"
        end
    end

    defmodule Command do
        use Signal.Command, 
            stream: {:uuid, TestAggregate}

        schema do
            field :id,      :string,   default: "command.id"
            field :uuid,    :string,   default: "command.uuid"
        end
            
        def handle(%Command{}=command, _params, _aggregate) do
            Event.from_struct(command)
        end

        def execute(%Command{uuid: uuid}, _params) do
            {:ok, uuid}
        end
    end


    describe "Command" do

        @tag :command
        test "has aggregate" do
            command = Command.new()
            aggregate = Stream.stream(command)
            assert elem(aggregate, 0) == "command.uuid"
            assert elem(aggregate, 1) == TestAggregate
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
        test "command name" do
            command = Command.new()
            assert Signal.Helper.atom_to_string(Command) === Signal.Name.name(command)
        end

        @tag :command
        test "command codec" do
            command = Command.new()
            encoded = Signal.Codec.encode(command)
            decoded = Signal.Codec.load(%Command{}, encoded |> elem(1))
            assert Signal.Result.ok?(encoded)
            assert Signal.Result.ok?(decoded)
            assert match?(^command, decoded |> elem(1))
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
