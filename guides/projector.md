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
    alias App.Bank.Events.Widthdrawn
    use Signal.Projector,
        app: App.Signal,
        topics: [Deposited, Widthdrawn, ...]

    # Project events to a Datastore
    def project(%Deposited{}) do
        App.Database.white_transaction(...)    
    end

    def project(%Widthdrawn{}) do
        App.Database.white_transaction(...)    
    end

end
```

### Definition

The `use Signal.Projector` Keyword list options

```elixir
    use Signal.Projector, [...options]
```

- `:app` Signal application module
- `:name` projector name if none it given the Module name will be used as process name
- `:start` starts processing events from current cursor position, `:beginning` starts processing events from event number 0
- `:topics` list, events to listern, event name can be string or atom
- `:timeout` number of microseconds of inactivity before projector hibernation
- `:shutdown`  how to shut down a projector, either immediately or by giving it time to shut down
- `:restart` when the projector should be restarted, defaults to :transient


### Callbacks

#### procject/1
Invoked on all events

