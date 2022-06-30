defmodule Signal.Processor.SagaTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store

    defmodule Accounts do

        use Signal.Aggregate

        blueprint do
            field :number,  :string,   default: "123"
            field :balance, :number,  default: 0
        end

        def apply(_event, _meta, %Accounts{}=act) do
            {:ok, act}
        end

    end

    defmodule Deposited do

        use Signal.Event,
            topic: "user.deposited",
            stream: {Accounts, :account}

        blueprint do
            field :account, :string,    default: "123"
            field :amount,  :number,    default: 0
        end

    end

    defmodule AccountOpened do
        use Signal.Event,
            stream: {Accounts, :account}

        blueprint do
            field :pid,     :number
            field :account, :string,   default: "123"
        end
    end

    defmodule AccountClosed do
        use Signal.Event,
            stream: {Accounts, :account}

        blueprint do
            field :account, :string,   default: "123"
        end
    end

    defmodule OpenAccount do

        use Signal.Command,
            stream: {Accounts, :account}

        blueprint do
            field :pid,     :number
            field :account, :string,   default: "123"
        end

        def handle(%OpenAccount{}=cmd, _params, %Accounts{}) do
            AccountOpened.from(cmd)
        end
    end

    defmodule Deposite do

        use Signal.Command,
            stream: {Accounts, :account}

        blueprint do
            field :account,     :string,    default: "123"
            field :amount,      :number,    default: 0
        end

        def handle(%Deposite{}=deposite, _params, %Accounts{}) do
            Deposited.from(deposite)
        end
    end

    defmodule CloseAccount do
        use Signal.Command,
            stream: {Accounts, :account}

        blueprint do
            field :account,     :string,   default: "123"
        end

        def handle(%CloseAccount{}=cmd, _params, %Accounts{}) do
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

        use Signal.Process.Manager,
            application: TestApp,
            topics: [AccountOpened, "user.deposited", AccountClosed]

        defstruct [:account, :amount, :pid]

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

        def apply(%AccountOpened{pid: pid}=ev, %ActivityNotifier{}=act) do
            state = %ActivityNotifier{act | pid: pid}
            acknowledge(state, ev)
            {:ok, state}
        end

        def apply(%Deposited{}=ev, %ActivityNotifier{amount: amt}=act) do
            acknowledge(act, ev)
            amount = ev.amount + amt
            if amount == 9000 do
                bonus = %Deposite{account: "123", amount: 1000}
                {:dispatch, bonus , %ActivityNotifier{act | amount: amount} }
            else
                {:ok, %ActivityNotifier{act | amount: amount} }
            end
        end

        def apply(%AccountClosed{}=ev, %ActivityNotifier{}=act) do
            acknowledge(act, ev)
            {:shutdown, act}
        end

        def error(%Deposite{}, _error, %ActivityNotifier{}=acc) do
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

    describe "Process" do

        @tag :process
        test "process should start stop and continue" do

            TestApp.dispatch(OpenAccount.new([pid: self()]), app: :saga)

            TestApp.dispatch(Deposite.new([amount: 5000]), app: :saga)

            assert_receive(%AccountOpened{account: "123"}, 1000)

            assert_receive(%Deposited{amount: 5000}, 1000)

            TestApp.dispatch(Deposite.new([amount: 4000]), app: :saga)

            assert_receive(%Deposited{amount: 4000}, 1000)
            assert_receive(%Deposited{amount: 1000}, 1000)

            TestApp.dispatch(CloseAccount.new([]), app: :saga, await: true)

            assert_receive(%AccountClosed{}, 1000)

            Process.sleep(2000)
            refute ActivityNotifier.alive?("123")
        end

    end

end
