defmodule Signal.Processor.SagaTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store

    defmodule Accounts do

        use Signal.Aggregate

        schema do
            field :number,      String.t,   default: "123"
            field :balance,     integer(),  default: 0
        end

        def apply(_event, _meta, %Accounts{}=act) do
            act
        end

    end

    defmodule Deposited do

        use Signal.Event,
            topic: "user.deposited",
            stream: {Accounts, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end

    end

    defmodule AccountOpened do
        use Signal.Event,
            stream: {Accounts, :account}

        schema do
            field :pid,         term()
            field :account,     String.t,   default: "123"
        end
    end


    defmodule Deposite do

        use Signal.Command,
            stream: {Accounts, :account}

        schema do
            field :account,     String.t,   default: "123"
            field :amount,      integer(),  default: 0
        end

        def handle(%Deposite{}=deposite, _params, %Accounts{}) do
            Deposited.from(deposite)
        end
    end

    defmodule OpenAccount do

        use Signal.Command,
            stream: {Accounts, :account}

        schema do
            field :pid,         term()
            field :account,     String.t,   default: "123"
        end

        def handle(%OpenAccount{}=cmd, _params, %Accounts{}) do
            AccountOpened.from(cmd)
        end
    end


    defmodule Router do

        use Signal.Router

        register Deposite
        register OpenAccount

    end

    defmodule TestApp do

        use Signal.Application,
            store: Store

        router Router

    end

    defmodule ActivityNotifier do

        use Signal.Process.Manager,
            application: TestApp,
            topics: [AccountOpened, "user.deposited"]

        defstruct [:account, :amount, :pid]

        def init(id) do
            struct(__MODULE__, [account: id, amount: 0])
        end

        def handle(%AccountOpened{account: account}) do
            {:start, account}
        end

        def handle(%Deposited{account: id, amount: 4000}) do
            {:continue, id}
        end

        def handle(%Deposited{amount: 5000, account: id}) do
            {:halt, id}
        end

        def apply(%AccountOpened{pid: pid}=ev, %ActivityNotifier{}=act) do
            IO.inspect(ev)
            Process.send(pid, ev, [])
            {:ok, %ActivityNotifier{act | pid: pid}}
        end

        def apply(%Deposited{amount: 4000}=ev, %ActivityNotifier{pid: pid, amount: 5000}=act) do
            IO.inspect(ev)
            Process.send(pid, ev, [])
            bonus = %Deposite{account: "123", amount: 1000}
            {:dispatch, bonus , %ActivityNotifier{act | amount: 9000} }
        end

        def apply(%Deposited{amount: 1000}=ev, %ActivityNotifier{pid: pid, amount: amt}=act) do
            IO.inspect(ev)
            Process.send(pid, ev, [])
            {:ok, %ActivityNotifier{act | amount: amt + 100} }
        end

        def halt(%Deposited{amount: 5000}=ev, %ActivityNotifier{pid: pid}=act) do
            IO.inspect(ev)
            Process.send(pid, ev, [])
            {:stop, %ActivityNotifier{act | amount: 5000}}
        end

        def stop(%Deposited{amount: 5000}=ev, %ActivityNotifier{pid: pid}=act) do
            IO.inspect(ev)
            Process.send(pid, ev, [])
            {:ok, %ActivityNotifier{act | amount: 5000}}
        end

        def error(%Deposite{amount: 1000}, _error, %ActivityNotifier{}=acc) do
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

            Process.sleep(200)
            refute ActivityNotifier.alive?("123")

            TestApp.dispatch(Deposite.new([amount: 4000]), app: :saga)

            assert_receive(%Deposited{amount: 4000}, 5000)
            assert_receive(%Deposited{amount: 1000}, 5000)
        end

    end

end
