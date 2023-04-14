## Aggregate

An Aggregate is a struct that hold a stream state
and can be mutatated after each stream event 
reduction

```elixir
defmodule App.Bank.Account do
    alias App.Bank.Account
    use Signal.Aggregate

    schema do
        field :account_id,  :uuid
        field :balance,     :float
    end

    #helper user defined method
    def deposite(%Account{}=account, amount) do
        %Account{...}
    end

    #helper user defined method
    def widthdraw(%Account{}=account, amount) do
        %Account{...}
    end

    ...

end
```

