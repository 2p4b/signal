defmodule Signal.Events.BrokerTest do
    use ExUnit.Case, async: true

    alias Signal.VoidStore
    alias Signal.Events.Event
    alias Signal.Events.Staged
    alias Signal.Stream.Broker
    alias Signal.Events.Recorder
    alias Signal.Stream.Consumer

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

    describe "Broker" do

        @tag :broker
        test "should start broker" do
            type = Stream.Type

            app = {TestApp, TestApp}

            broker =
                Signal.Stream.Supervisor.prepare_broker(app, type)
                |> GenServer.call(:state, 5000)

            assert match?(%Signal.Stream.Broker{
                index: 0,
                type: ^type,
                app: ^app,
                store: VoidStore,
                consumers: [],
            }, broker)

        end

        @tag :broker
        test "should recieve events from stream" do

            app = {TestApp, TestApp}
            deposited = Deposited.new([amount: 5000]) 

            stream = Signal.Stream.stream(deposited)

            action = 
                [amount: 5000]
                |> Deposite.new()
                |> Signal.Execution.Task.new([app: app])
                |> Signal.Command.Action.from()

            event = Signal.Events.Event.new(deposited, action, 1) 

            staged = %Staged{
                stage: self(),
                stream: stream, 
                events: [event], 
                version: 1, 
            }

            Recorder.record(app, action, staged)

            %Consumer{ack: 0} = Broker.stream_from(app, stream, 0)

            Broker.acknowledge(app, stream, 0)

            assert_receive(%Event{ payload: ^deposited })
        end

    end

end



