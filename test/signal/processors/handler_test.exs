defmodule Signal.Processor.HandlerTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store
    alias Signal.Transaction
    alias Signal.Stream.Stage

    defmodule TestApp do
        use Signal.Application,
            store: Store
    end

    defmodule Accounts do
        use Blueprint.Struct

        schema do
            field :number,      :string,    default: "123"
            field :balance,     :number,    default: 0
        end
    end

    defmodule Deposited do
        use Signal.Event,
            stream: {Accounts, :account}

        schema do
            field :account, :string,    default: "123"
            field :amount,  :number,    default: 0
        end
    end


    defmodule Deposite do
        use Signal.Command,
            stream: {Accounts, :account}

        schema do
            field :account, :string,    default: "123"
            field :amount,  :number,    default: 0
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

        def handle_call(:intercept, {pid, _ref}, _state) do
            {:reply, :ok, pid}
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

            :ok = GenServer.call(TestHandler, :intercept)

            stream = Signal.Stream.stream(deposited)

            event1 = Signal.Stream.Event.new(deposited, [index: 1])

            event2 = Signal.Stream.Event.new(deposited2, [index: 2])

            staged1 = %Stage{
                stage: self(),
                stream: stream,
                events: [event1],
                position: event1.index,
            }
            |> Transaction.new()

            staged2 = %Stage{
                stage: self(),
                stream: stream,
                events: [event2],
                position: event2.index,
            }
            |> Transaction.new()

            assert :ok == Signal.Store.Writer.commit(TestApp, staged1, [])
            assert_receive(%Deposited{ amount: 5000 }, 1000)

            assert :ok == Signal.Store.Writer.commit(TestApp, staged2, [])
            assert_receive(%Deposited{ amount: 4000 }, 1000)
        end

    end

end
