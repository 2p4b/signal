defmodule Signal.Events.AggregateTest do
    use ExUnit.Case

    alias Signal.Void.Store
    alias Signal.Event

    defmodule TestApp do
        use Signal.Application,
            store: Store
    end

    defmodule Account do
        use Draft.Schema
        schema do
            field :number,      :number,    default: "123"
            field :balance,     :number,    default: 0
        end
    end

    defmodule Deposite do
        use Signal.Command,
            stream: {:account, Account}

        schema do
            field :account,     :string,   default: "123"
            field :amount,      :string,    default: 0
        end
    end

    defmodule Deposited do
        use Signal.Event,
            stream: {:account, Account}

        schema do
            field :account, :string,    default: "123"
            field :amount,  :number,    default: 0
        end
    end


    defimpl Signal.Stream.Reducer, for: Account do

        def apply(%Account{balance: balance}=account, %Deposited{amount: amount}) do
            {:ok, %Account{ account | balance: balance + amount }}
        end

    end

    setup_all do
        start_supervised(Store)
        :ok
    end


    setup do
        {:ok, _pid} = start_supervised(TestApp)
        stream = Signal.Stream.stream(Deposited.new())
        {:via, _, _} =
            TestApp
            |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)
        :ok
    end

    describe "Aggregate" do

        @tag :aggregate
        test "should initialialize state" do
            deposited = Deposited.new([amount: 1])

            stream = Signal.Stream.stream(deposited)

            aggregate =
                TestApp
                |> Signal.Aggregates.Supervisor.prepare_aggregate(stream)

            {:ok, state} = Signal.Aggregates.Aggregate.state(aggregate)

            assert match?(%Account{number: "123"}, state)

            event1 =
                struct(Event, [])
                |> Map.put(:topic, Signal.Name.name(deposited))
                |> Map.put(:data, Map.from_struct(deposited))
                |> Map.put(:number, 1)
                |> Map.put(:position, 1)

            event2 =
                struct(Event, [])
                |> Map.put(:topic, Signal.Name.name(deposited))
                |> Map.put(:data, Map.from_struct(deposited))
                |> Map.put(:number, 2)
                |> Map.put(:position, 2)


            Signal.Aggregates.Aggregate.apply(aggregate, event1)

            Signal.Aggregates.Aggregate.apply(aggregate, event2)

            {:ok, account} = Signal.Aggregates.Aggregate.state(aggregate)

            assert match?(%Account{number: "123", balance: 2}, account)

            deposited = Deposited.new([amount: 3])

            event3 =
                struct(Event, [])
                |> Map.put(:topic, Signal.Name.name(deposited))
                |> Map.put(:data, Map.from_struct(deposited))
                |> Map.put(:number, 3)
                |> Map.put(:position, 3)

            task =
                Task.async(fn ->
                    Signal.Aggregates.Aggregate.await(aggregate, 3)
                end)

            Signal.Aggregates.Aggregate.apply(aggregate, event3)

            {:ok, account} = Task.await(task, :infinity)

            assert match?(%Account{number: "123", balance: 5}, account)
        end
    end

end
