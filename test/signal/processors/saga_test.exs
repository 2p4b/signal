defmodule Signal.Processor.SagaTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store

    defmodule Account do

        use Signal.Aggregate

        schema do
            field :number,  :string,  default: "saga.123"
            field :balance, :number,  default: 0
        end

        def apply(_event, _meta, %Account{}=act) do
            {:ok, act}
        end

    end

    defmodule Deposited do

        use Signal.Event,
            stream: {Account, :account}

        schema do
            field :account, :string,    default: "saga.123"
            field :amount,  :number,    default: 0
        end

    end

    defmodule AccountOpened do
        use Signal.Event,
            stream: {Account, :account}

        schema do
            field :pid,     :any
            field :account, :string,   default: "saga.123"
        end
    end

    defmodule AccountClosed do
        use Signal.Event,
            stream: {Account, :account}

        schema do
            field :account, :string,   default: "saga.123"
        end
    end

    defmodule OpenAccount do

        use Signal.Command,
            stream: {Account, :account}

        schema do
            field :pid,     :any
            field :account, :string,   default: "saga.123"
        end

        def handle(%OpenAccount{}=cmd, _params, %Account{}) do
            AccountOpened.from(cmd)
        end
    end

    defmodule Deposite do

        use Signal.Command,
            stream: {Account, :account}

        schema do
            field :account,     :string,    default: "saga.123"
            field :amount,      :number,    default: 0
        end

        def handle(%Deposite{}=deposite, _params, %Account{}) do
            Deposited.from(deposite)
        end
    end

    defmodule CloseAccount do
        use Signal.Command,
            stream: {Account, :account}

        schema do
            field :account,     :string,   default: "saga.123"
        end

        def handle(%CloseAccount{}=cmd, _params, %Account{}) do
            AccountClosed.from(cmd)
        end
    end


    defmodule Router do

        use Signal.Router

        register Deposite
        register OpenAccount
        register CloseAccount

    end

    defmodule TestApp do

        use Signal.Application,
            store: Store

        router Router

    end

    defmodule ActivityNotifier do

        use Signal.Process,
            application: TestApp,
            topics: [AccountOpened, Deposited, AccountClosed]

        schema do
            field :account,   :string
            field :amount,    :number  
            field :pid,       :any
        end

        def init(id) do
            struct(__MODULE__, [account: id, amount: 0])
        end

        def handle(%AccountOpened{account: account}) do
            {:start, account}
        end

        def handle(%Deposited{account: id}) do
            {:apply, id}
        end

        def handle(%AccountClosed{account: id}) do
            {:apply, id}
        end

        defp acknowledge(%ActivityNotifier{pid: pid}, event) do
            Process.send(pid, event, [])
        end

        def apply(%AccountOpened{pid: pid}=ev, _,  %ActivityNotifier{}=act) do
            state = %ActivityNotifier{act | pid: pid}
            acknowledge(state, ev)
            {:ok, state}
        end

        def apply(%Deposited{}=ev, _,  %ActivityNotifier{amount: amt}=act) do
            acknowledge(act, ev)
            amount = ev.amount + amt
            if amount == 9000 do
                bonus = %Deposite{account: "saga.123", amount: 1000}
                {:dispatch, bonus , %ActivityNotifier{act | amount: amount} }
            else
                {:ok, %ActivityNotifier{act | amount: amount} }
            end
        end

        def apply(%AccountClosed{}=ev, _,  %ActivityNotifier{}=act) do
            acknowledge(act, ev)
            {:shutdown, act}
        end

        def error(%Deposite{}, _error, _,  %ActivityNotifier{}=acc) do
            {:ok, acc}
        end

    end

    setup_all do
        start_supervised(Store)
        :ok
    end

    setup do
        {:ok, _pid} = start_supervised({TestApp, name: :saga})
        {:ok, _pid} = start_supervised({ActivityNotifier, app: :saga})
        :ok
    end

    describe "Saga" do

        @tag :saga
        test "process should start stop and continue" do

            TestApp.dispatch(OpenAccount.new([pid: self()]), app: :saga)

            TestApp.dispatch(Deposite.new([amount: 5000]), app: :saga)

            assert_receive(%AccountOpened{account: "saga.123"}, 1000)

            assert_receive(%Deposited{amount: 5000}, 1000)

            TestApp.dispatch(Deposite.new([amount: 4000]), app: :saga)

            assert_receive(%Deposited{amount: 4000}, 1000)
            assert_receive(%Deposited{amount: 1000}, 1000)

            TestApp.dispatch(CloseAccount.new([]), app: :saga, await: true)

            assert_receive(%AccountClosed{}, 1000)

            Process.sleep(500)
            refute TestApp.process_alive?(ActivityNotifier, "saga.123")
        end

    end

end
