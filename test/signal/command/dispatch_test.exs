defmodule Signal.Command.DispatchTest do

    use ExUnit.Case

    alias Signal.Task
    alias Signal.Result
    alias Signal.Void.Store
    alias Signal.Stream.History

    defmodule TestApplication do
        use Signal.Application,
            store: Store

        queue :default, timeout: 5000, retries: 0
    end

    defmodule Account do
        use Blueprint.Struct

        schema do
            field :number,      :string,    default: "123"
            field :balance,     :number,    default: 0
        end
    end

    defmodule Deposited do
        use Signal.Event,
            stream: {Account, :account}

        schema do
            field :account, :string,   default: ""
            field :amount,  :number,  default: 0
        end
    end

    defmodule Deposite do
        use Signal.Command,
            stream: {Account, :account}

        schema do
            field :account, :string,    default: "123"
            field :amount,  :number,    default: 0
        end

        def handle(%Deposite{}=deposite, _params, %Account{number: "123"}) do
            Deposited.from_struct(deposite)
        end
    end

    defimpl Signal.Stream.Reducer, for: Account do
        def apply(%Account{balance: balance}=account, %Deposited{amount: amt}) do
            {:ok, %Account{ account | balance: balance + amt }}
        end
    end

    setup do
        {:ok, _pid} = start_supervised(Store)
        {:ok, _pid} = start_supervised(TestApplication)
        #{:ok, _} = Application.ensure_all_started(TestApplication)
        :ok
    end

    describe "Dispatcher" do

        @tag :dispatcher
        test "Signal.Command.Dispatcher.execute/1" do
            result =
                [amount: 1000]
                |> Deposite.new()
                |> Task.new([
                    timeout: :infinity,
                    app: TestApplication
                ])
                |> Signal.Command.Dispatcher.execute()

            assert match?({:ok, %{}}, result)
        end

        @tag :dispatcher
        test "Signal.Command.Dispatcher.process/1" do
            {:ok, histories} =
                [amount: 2000]
                |> Deposite.new()
                |> Task.new([
                    timeout: :infinity,
                    app: TestApplication
                ])
                |> Signal.Command.Dispatcher.process()

            [%History{}=history] = histories
            assert length(history.events) == 1
            assert history.version == 1
        end

        @tag :dispatcher
        test "Signal.Command.Dispatcher.dispatch/1" do
            amount = 3000

            {:ok, result} =
                [amount: amount]
                |> Deposite.new()
                |> Task.new([
                    await: true,
                    timeout: :infinity,
                    consistent: true,
                    app: TestApplication
                ])
                |> Signal.Command.Dispatcher.dispatch()

            assert match?(%Result{
                aggregates: [%Account{number: "123"}],
                assigns: %{},
                result: nil,
            }, result)
        end

    end

end
