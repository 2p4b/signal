defmodule Signal.Processor.SagaTest do
    use ExUnit.Case, async: true

    alias Signal.Void.Store


    defmodule Account do
        use Signal.Aggregate

        schema do
            field :number,  :string,  default: "saga.123"
            field :balance, :number,  default: 0
        end
    end

    defmodule Deposited do
        use Signal.Event,
            stream: {:account, Account}

        schema do
            field :reason,  :string,   default: "deposite"
            field :account, :string,    default: "saga.123"
            field :amount,  :number,    default: 0
        end

        def apply(_event, %Account{}=act) do
            {:ok, act}
        end
    end

    defmodule AccountOpened do
        use Signal.Event,
            stream: {:account, Account}

        schema do
            field :pid,     :any
            field :promo,   :number,  default: 0
            field :account, :string,  default: "saga.123"
        end

        def apply(_event, %Account{}=act) do
            {:ok, act}
        end
    end

    defmodule AccountClosed do
        use Signal.Event,
            stream: {:account, Account}

        schema do
            field :reason,  :string,   default: "Reason bezos"
            field :account, :string,   default: "saga.123"
        end

        def apply(_event, %Account{}=act) do
            {:ok, act}
        end
    end

    defmodule OpenAccount do
        use Signal.Command,
            stream: {:account, Account}

        schema do
            field :pid,     :any
            field :account, :string,   default: "saga.123"
        end

        def handle(%OpenAccount{}=cmd, _params, %Account{}) do
            AccountOpened.from_struct(cmd)
        end
    end

    defmodule Deposite do
        use Signal.Command,
            stream: {:account, Account}

        schema do
            field :account,     :string,    default: "saga.123"
            field :amount,      :number,    default: 0
            field :reason,      :string,    default: ""
        end

        def handle(%Deposite{}=deposite, _params, %Account{}) do
            Deposited.from_struct(deposite)
        end
    end

    defmodule CloseAccount do
        use Signal.Command,
            stream: {:account, Account}

        schema do
            field :account,     :string,   default: "saga.123"
        end

        def handle(%CloseAccount{}=cmd, _params, %Account{}) do
            AccountClosed.from_struct(cmd)
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
            app: TestApp,
            topics: [AccountOpened, Deposited, AccountClosed]

        schema do
            field :account,   :string
            field :count,     :number,  default: 0
            field :promotion, :map,     default: %{target: 0, cliamed: false, amount: 1000}
            field :amount,    :number  
            field :pid,       :any
        end

        def init(id) do
            struct(__MODULE__, [account: id, amount: 0, count: 0])
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

        def handle_event(%AccountOpened{pid: pid}=ev, %ActivityNotifier{}=self) do
            state = 
                self 
                |> increment_count()
                |> set_account_pid(pid) 
                |> set_promotion(ev.promo)

            notify_tester(state, ev)
            {:ok, state}
        end

        def handle_event(%Deposited{}=ev, %ActivityNotifier{}=self) do
            notify_tester(self, ev)

            state = 
                self 
                |> deposite_amount(ev.amount) 
                |> increment_count()

            if state.promotion.cliamed == false and state.promotion.target <= state.amount do
                reason = "bonus"
                promo_amount = state.promotion.amount
                promo_bonus = %{"account" => ev.account, "amount" => promo_amount, "reason" => reason}
                {:dispatch, Deposite.new(promo_bonus), claim_promo(state, promo_amount)}
            else
                {:ok, state}
            end
        end

        def handle_event(%AccountClosed{}=ev,  %ActivityNotifier{}=self) do
            notify_tester(self, ev)
            {:stop, self}
        end

        def handle_error(%Deposite{}, _error,  %ActivityNotifier{}=self) do
            {:ok, self}
        end

        defp set_promotion(%ActivityNotifier{}=self, target) do
            %ActivityNotifier{self | promotion: %{self.promotion | target: target, cliamed: false}}
        end

        defp notify_tester(%ActivityNotifier{pid: pid}=self, event) do
            Process.send(pid, event, [])
            self 
        end

        defp set_account_pid(%ActivityNotifier{pid: nil}=self, pid) do
            %ActivityNotifier{self | pid: pid}
        end

        defp increment_count(%ActivityNotifier{count: count}=self) do
            %ActivityNotifier{self | count: count + 1}
        end

        defp deposite_amount(%ActivityNotifier{amount: amount}=self, value) do
            %ActivityNotifier{self | amount: amount + value}
        end

        defp claim_promo(%ActivityNotifier{promotion: promo}=self, amount) do
            %ActivityNotifier{self | amount: amount, promotion: %{promo | cliamed: true}}
        end

    end

    setup_all do
        start_supervised(Store)
        {:ok, _pid} = start_supervised(TestApp)
        {:ok, _pid} = start_supervised(ActivityNotifier)
        :ok
    end

    describe "Saga" do

        @tag :saga
        test "process should start stop and continue" do
            first_amount = 5000
            second_amount = 4000
            promotion_target = first_amount + second_amount
            
            TestApp.dispatch(OpenAccount.new([pid: self(), promo: promotion_target]))

            TestApp.dispatch(Deposite.new([amount: 5000]))

            assert_receive(%AccountOpened{account: "saga.123"}, 3000)

            assert_receive(%Deposited{amount: ^first_amount}, 10000)

            TestApp.dispatch(Deposite.new([amount: second_amount]))

            assert_receive(%Deposited{amount: ^second_amount}, 10000)

            # TODO: add test for shutting down the process
        end

    end

end
