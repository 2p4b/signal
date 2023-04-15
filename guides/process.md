## Process

An Process is a long running Saga that syncronizes aggregate states.
Processes in turn do have their own state.
Process performs syncronization by listerning to event,
firing actions, dispatching commands

**Yes Each Saga is just a state machine**

All sagas within a process name **MUST** have a unique id
                                    
Use case:
Say two users in a bank want to send money to each other. A Process is
great way to syncronize those two states. In this case the first event that
starts the Saga will be a `Transfer` event with all the relevant infomation 
next is a `Debited` event on the sender Account stream and finally 
and the next Event is a `Credited` event on the receiver Account stream

note: Just an example not real!

```elixir
defmodule App.Bank.Process.TransferProcess do
    alias App.Bank.Events.Debited
    alias App.Bank.Events.Transfer
    alias App.Bank.Events.Credited

    alias App.Bank.Commands.Debit
    alias App.Bank.Commands.Credit

    use Signal.Process,
        app: App.Signal,
        name: "credit.transfer.transaction",
        topics: [Debited, Transfer, Credited]

    schema do
        field :transaction_id,          :uuid,
        field :amount,                  :amount,
        field :sender_account_id,       :uuid
        field :recipient_account_id,    :uuid
        field :sender_debited,          :boolean,   default: false
        field :recipient_credited,      :boolean,   default: false
    end

    def handle(%Transfer{transaction_id: transaction_id}) do
        {:start, transaction_id}
    end

    def handle(%Debited{transaction_id: transaction_id}) do
        {:apply, transaction_id}
    end

    def handle(%Credited{transaction_id: transaction_id}) do
        {:apply, transaction_id}
    end

    # initialize the state
    def init(id) do
        struct(__MODULE__, [transaction_id: id])
    end

    def handle_event(%Transfer{}=trxn, process) do
        # Blueprint struct casting
        process = __MODULE__.from_struct(trxn)

        params = 
            Map.new()
            |> Map.put("type", "transfer")
            |> Map.put("amount", process.amount)
            |> Map.put("transaction_id", process.transaction_id)
            |> Map.put("sender_account_id", process.sender_account_id)
            |> Map.put("recipient_account_id", process.recipient_account_id)

        {:action, {"debit-sender-account", params}, process}, 
    end

    def handle_event(%Debited{}, process) do
        process = %__MODULE__{process| sender_debited: true}
        params = 
            Map.new()
            |> Map.put("type", "transfer")
            |> Map.put("amount", process.amount)
            |> Map.put("account_id", process.recipient_account_id)
            |> Map.put("transaction_id", process.transaction_id)

        {:action, {"credit-recipient-account", params}, process}, 
    end

    def handle_event(%Credited{}, process) do
        process = %__MODULE__{process| recipient_credited: true}
        {:stop, process}
    end

    def handle_action({"debit-sender-account", params}, process) do
        {:dispatch, Debit.new(params)}
    end

    def handle_action({"credit-recipient-account", params}, process) do
        {:dispatch, Credit.new(params)}
    end

    # Stop process if :insufficient_funds error is returned 
    # from Debit dispatch command
    def handle_error({:insufficient_funds, %Debit{}}, _action, process) do
        ## Do some reporting maybe and stop the saga
        {:stop, process}
    end

    # Keep retrying to credit recipient account
    def handle_error({reason, %Credit{}}, action, process) do
        {:retry, action, process}
    end

end
```

#### handle/1
We start a Process saga by calling `handle/1` to route the 
event to the right Saga instance

- `{:start, saga_id}` will start a saga instance with the id within the process namespace. If a saga alread exist with the id the event will be routed to the existing saga instance
- `{:apply, saga_id}` will route the event to saga instance with the id, if no saga exists withing the namespace with the same id the event will be ignored
- `:skip` will ignore the event

#### handle_event/2
`handle_event/2` callback is called once and event is routed  to a saga instance

- `{:ok, state}` save the saga new state 
- `{:action, {action_name, action_params}, state}` fire and action with new saga state. All actions are placed in an FIFO action queue
- `{:stop, state}` stop the saga process from processing any more events, Once all actions in the queue are processed the saga state is deleted



#### handle_action/2
`handle_action/2` processes the actions FIFO queue one at a time

- `{:ok, state}` save the saga new state 
- `{:dipatch, command}` dispatch new command
- `{:action, {action_name, action_params}, state}` puts action in the FIFO queue, if there are any actions in the action queue, those actions will handled first

- `{:retry, {action_name, action_params}, state}` puts action back in FIFO queue at the same position, and will be handled immediately before any existing actions


#### handle_error/3
`handle_error/3` handles errors from dispatch command

```elixir
    def handle_error({reason, command}, {action_name, action_params}, process)
```

- `{:ok, state}` save the saga new state 
- `{:action, {action_name, action_params}, state}` puts action in the FIFO queue, if there are any actions in the action queue, those actions will handled first
- `{:retry, {action_name, action_params}, state}` puts action back in FIFO queue at the same position, and will be handled immediately before any existing actions
- `{:stop, state}` stop the saga process from processing any more events, Once all actions in the queue are processed the saga state is deleted
