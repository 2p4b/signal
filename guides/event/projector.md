## Projector

A Projector is an Event Handler with only the `project` callback behaviour 
and without a state 

Once the `project` callback returns the event is acknowleged
raising or throwing exceptions prevents the event from being acknowleged
Well that will crash the Projector process too soo.....

```elixir
defmodule App.Bank.TransactionLogger do
    alias App.Database
    alias App.Bank.Events.Deposited
    use Signal.Projector,
        app: App.Signal,
        topics: [Deposited]

    # Project events to a Datastore
    def project(%Deposited{}) do
        App.Database.white_transaction(...)    
    end

end
```

