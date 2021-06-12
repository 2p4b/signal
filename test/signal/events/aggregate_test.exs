defmodule Signal.Events.AggregateTest do
    use ExUnit.Case

    alias Signal.VoidStore
    alias Signal.Events.Event

    defmodule TestApp do
        use Signal.Application,
            store: VoidStore
    end

    defmodule Accounts do
        use Signal.Aggregate

        schema do
            field :number,      String.t,   default: "123"
            field :balance,     integer(),  default: 0
        end

    end

    defmodule Deposite do
        use Signal.Command,
            stream: {Accounts, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
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


    defimpl Signal.Stream.Reducer, for: Accounts do

        def apply(%Accounts{balance: balance}=account, _meta, %Deposited{amount: amount}) do
            %Accounts{ account | balance: balance + amount }
        end

    end

    setup_all do
        start_supervised(VoidStore)
        :ok
    end


    setup do
        {:ok, _pid} = start_supervised(TestApp)
        stream = Signal.Stream.stream(Deposited.new())
        {:via, _, _} =
            {TestApp, TestApp}
            |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
        :ok
    end

    describe "Aggregate" do

        @tag :aggregate
        test "should initialialize state" do
            stream = Signal.Stream.stream(Deposited.new())
            aggregate =
                {TestApp, TestApp}
                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
                |> Signal.Aggregates.Aggregate.state()

            assert match?(%Accounts{number: "123"}, aggregate)

            deposited = Deposited.new([amount: 1])

            stream = Signal.Stream.stream(deposited)

            action =
                Deposite.new([amount: 1])
                |> Signal.Execution.Task.new([app: TestApp])
                |> Signal.Command.Action.from()

            event1 =
                Event.new(deposited, action, 1)
                |> Event.index(1)

            event2 =
                Event.new(deposited, action, 2)
                |> Event.index(20)

            {TestApp, TestApp}
            |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
            |> Signal.Aggregates.Aggregate.apply(event1)

            {TestApp, TestApp}
            |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
            |> Signal.Aggregates.Aggregate.apply(event2)

            account =
                {TestApp, TestApp}
                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
                |> Signal.Aggregates.Aggregate.state()

            assert match?(%Accounts{number: "123", balance: 2}, account)

            deposited = Deposited.new([amount: 3])

            stream = Signal.Stream.stream(deposited)

            action =
                Deposite.new([amount: 3])
                |> Signal.Execution.Task.new([app: TestApp])
                |> Signal.Command.Action.from()

            event =
                Event.new(deposited, action, 3)
                |> Event.index(1)

            aggregate =
                {TestApp, TestApp}
                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)

            task =
                Task.async(fn ->
                    Signal.Aggregates.Aggregate.await(aggregate, 3)
                end)

            Signal.Aggregates.Aggregate.apply(aggregate, event)

            account = Task.await(task, :infinity)

            assert match?(%Accounts{number: "123"}, account)
        end
    end

end
