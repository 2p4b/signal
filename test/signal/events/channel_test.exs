defmodule Signal.Events.ChannelTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store
    alias Signal.Stream.Event
    alias Signal.Events.Staged
    alias Signal.Channels.Channel

    defmodule TestApp do

        use Signal.Application,
            store: Store
    end

    defmodule Accounts do

        use Signal.Type

        schema do
            field :number,      String.t,   default: "123"
            field :balance,     integer(),  default: 0
        end

    end

    defmodule Deposited do

        use Signal.Event,
            stream: {Accounts, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end
    end


    defmodule Deposite do

        use Signal.Command,
            stream: {Accounts, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end

        def handle(%Deposite{}=deposite, _param, %Accounts{number: "123", balance: 0}) do
            Deposited.from(deposite)
        end
    end

    setup_all do
        start_supervised(Store)
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

            deposited = Deposited.new([amount: 5000])

            topic = Signal.Helper.module_to_string(Deposited)

            TestApp.subscribe(__MODULE__, [topics: [topic]])

            %Signal.Subscription{} = Channel.subscribe(app, "Test.Channel", topic)

            event = Signal.Events.Event.new(deposited, [])

            staged =
                %Staged{
                    events: [event],
                    version: 1,
                    stream: Signal.Stream.stream(deposited),
                    stage: self()
                }


            TestApp.publish([staged])

            data = Signal.Codec.encode(deposited)

            assert_receive(%Event{ data: ^data })
        end

    end

end
