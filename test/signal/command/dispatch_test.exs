defmodule Signal.Command.DispatchTest do

    use ExUnit.Case

    alias Signal.Result
    alias Signal.VoidStore
    alias Signal.Stream.History
    alias Signal.Execution.Task

    defmodule TestApplication do
        use Signal.Application,
            store: VoidStore

        queue :default, timeout: 5000, retries: 0
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
            field :account,     String.t,   default: ""
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

    defimpl Signal.Stream.Reducer, for: Accounts do
        def apply(%Accounts{balance: balance}=account, _meta, %Deposited{amount: amt}) do
            %Accounts{ account | balance: balance + amt }
        end
    end

    setup do
        {:ok, _pid} = start_supervised(VoidStore)
        {:ok, _pid} = start_supervised(TestApplication)
        :ok
    end

    describe "Dispatcher" do

        @tag :dispatcher
        test "should process task" do
            result =
                Deposite.new([amount: 5000])
                |> Signal.Execution.Task.new([
                    timeout: :infinity,
                    app: {TestApplication, TestApplication}
                ])
                |> Signal.Command.Dispatcher.execute()

            assert match?({:ok, %{}}, result)
        end

        @tag :dispatcher
        test "should execute task" do
            {:ok, histories} =
                Deposite.new([amount: 5000])
                |> Signal.Execution.Task.new([
                    timeout: :infinity,
                    app: {TestApplication, TestApplication}
                ])
                |> Signal.Command.Dispatcher.process()

            [%History{}=history] = histories
            assert length(history.events) == 1
            assert history.version == 1
        end

        @tag :dispatcher
        test "should dispatch and return result" do

            command = Deposite.new([amount: 5000])

            task = Task.new(command, [
                await: true,
                consistent: true,
                app: {TestApplication, TestApplication}
            ])

            result = Signal.Command.Dispatcher.dispatch(task)

            assert match?(%Result{
                aggregates: [ %Accounts{ number: "123", balance: 5000 } ],
                assigns: %{},
                result: nil,
            }, result)
        end

    end

end
