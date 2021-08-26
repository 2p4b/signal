defmodule Signal.Processor.HandlerTest do
    use ExUnit.Case, async: true

    alias Signal.Handler
    alias Signal.Void.Store
    alias Signal.Stream.Event
    alias Signal.Events.Staged

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

        def handle(%Deposite{}=deposite, _params, %Accounts{number: "123", balance: 0}) do
            Deposited.from(deposite)
        end
    end

    defmodule TestHandler do
        use Signal.Handler,
            application: TestApp,
            topics: [Deposited]

        def init(sub, _opts) do
            {:ok, sub}
        end

        def handle_call(:intercept, {pid, _ref}, state) do
            {:reply, state, pid}
        end

        def handle_event(event, _meta, pid) do
            Process.send(pid, event, [])
            {:noreply, pid}
        end

    end

    setup_all do
        start_supervised(Store)
        :ok
    end

    setup do
        {:ok, _pid} = start_supervised(TestApp)
        {:ok, _pid} = start_supervised(TestHandler)
        :ok
    end

    describe "Handler" do

        @tag :handler
        test "should recieve events from topics" do

            deposited = Deposited.new([amount: 5000])

            deposited2 = Deposited.new([amount: 4000])

            GenServer.call(TestHandler, :intercept)

            stream = Signal.Stream.stream(deposited)

            event1 = Signal.Events.Event.new(deposited, [])

            event2 = Signal.Events.Event.new(deposited2, [])

            staged1 = %Staged{
                stage: self(),
                stream: stream,
                events: [event1],
                version: 1,
            }

            staged2 = %Staged{
                stage: self(),
                stream: stream,
                events: [event2],
                version: 2,
            }

            TestApp.publish(staged1, [])
            assert_receive(%Deposited{ amount: 5000 })

            TestApp.publish(staged2, [])
            assert_receive(%Deposited{ amount: 4000 })
            Process.sleep(1000)
        end

    end

end
