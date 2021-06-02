defmodule Signal.Events.RecorderTest do
    use ExUnit.Case, async: true

    alias Signal.VoidStore
    alias Signal.Events.Staged
    alias Signal.Events.Record
    alias Signal.Stream.History
    alias Signal.Events.Recorder

    defmodule TestApp do
        use Signal.Application, 
            store: VoidStore
    end

    defmodule Account do

        use Signal.Type

        schema do
            field :number,      String.t,   default: "123"
            field :balance,     integer(),  default: 0
        end
    end

    defmodule Deposited do

        use Signal.Event,
            stream: {Account, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end
    end

    defmodule Deposite do

        use Signal.Command,
            stream: {Account, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end

        def handle(%Deposite{}=deposite, _param, %Account{number: "123", balance: 0}) do
            Deposited.from(deposite)
        end
    end

    setup_all do
        start_supervised(VoidStore)
        :ok
    end

    setup do
        {:ok, _pid} = start_supervised(TestApp)
        :ok
    end

    describe "Recorder" do

        @tag :recorder
        test "should start recorder" do

            app = {TestApp, TestApp}
            deposited = Deposited.new([amount: 5000]) 

            stream = Signal.Stream.stream(deposited)

            action = 
                [amount: 5000]
                |> Deposite.new()
                |> Signal.Execution.Task.new([app: app])
                |> Signal.Command.Action.from()

            event = Signal.Events.Event.new(deposited, action, 1) 

            stage =
                %Staged{events: [event], version: 1, stream: stream, stage: self()}

            %Record{histories: [ history ]} = Recorder.record(app, action, [stage])

            assert match?(%History{}, history)
        end

    end

end

