defmodule Signal.Events.ChannelTest do
    use ExUnit.Case, async: true

    alias Signal.VoidStore
    alias Signal.Events.Event
    alias Signal.Events.Staged
    alias Signal.Events.Recorder
    alias Signal.Channels.Channel

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
        {:ok, _pid} = start_supervised({TestApp, name: :channel})
        :ok
    end

    describe "Channel" do

        @tag :channel
        test "should start channel" do
            name = "Test.Channel"
            topic = "Test.Topic"

            subscription = 
                {TestApp, :channel}
                |> Signal.Channels.Channel.subscribe(name, topic)

            assert match?(%Signal.Subscription{
                channel: ^name, 
                topics: [ ^topic ]
            }, subscription)

        end

        @tag :channel
        test "should recieve events from bus" do
            app = {TestApp, :channel}
            reduction = 1

            deposited = Deposited.new([amount: 5000]) 

            topic = Signal.Helper.module_to_string(Deposited)

            %Signal.Subscription{} = Channel.subscribe(app, "Test.Channel", topic)

            action = 
                [amount: 5000]
                |> Deposite.new()
                |> Signal.Execution.Task.new([app: app])
                |> Signal.Command.Action.from()

            event = Signal.Events.Event.new(deposited, action, reduction) 

            staged =
                %Staged{
                    events: [event], 
                    version: 1, 
                    stream: Signal.Stream.stream(deposited), 
                    stage: self()
                }

            Recorder.record(app, action, staged)

            data = Signal.Codec.encode(deposited)

            assert_receive(%Event{ data: ^data })
        end

    end

end


