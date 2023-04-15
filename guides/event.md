## Events

Events are applied to their respective stream aggregates serially

```elixir
defmodule App.Bank.Events.Deposited do
    alias App.Bank.Account
    alias App.Bank.Events.Deposited

    # [required] :stream
    use Signal.Command,
        stream: {Account, :account_id}

    schema do
        field :account_id, :uuid
        field :amount,     :float
        field :timstamp,   :datetime
    end

    # Apply event and return next aggregate state
    def apply(%Deposited{}=deposite, %Account{}=account) do
        %Account{...}
    end

    def apply(%Deposited{}=deposite, %Account{}=account) do
        # Advance aggregate to next state
        # and hibernate aggregate process
        Signal.Aggregate.sleep(%Account{...})
    end

    def apply(%Deposited{}=deposite, %Account{}=account) do
        # Advance aggregate to next state
        # and hibernate aggregate process after 1000ms
        Signal.Aggregate.sleep(%Account{...}, timeout: 1000)
    end

    def apply(%Deposited{}=deposite, %Account{}=account) do
        # Advance aggregate to next state and
        # return snapshot state point
        Signal.Aggregate.snapshot(%Account{...})
    end

    def apply(%Deposited{}=deposite, %Account{}=account) do
        # Advance aggregate to next state and
        # return snapshot state point 
        # and hibernate aggregate process
        Signal.Aggregate.snapshot(%Account{...}, :sleep)
    end

    def apply(%Deposited{}=deposite, %Account{}=account) do
        # Advance aggregate to next state and
        # return snapshot state point 
        # and hibernate aggregate process after 1000ms
        Signal.Aggregate.snapshot(%Account{...}, timeout: 1000)
    end

end
```

### Definition

The `use Signal.Event` accepts seven Keyword list options

```elixir
    use Signal.Event, [...options]
```
- `:stream` [required] command event stream tag



#### Apply event
The `apply` callback is called from the event stream aggregate process (`{Account, :account_id}`)
and return the next stream aggregate state


