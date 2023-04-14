# Router

Routers are used to register commands and processing pipelines

This router defines a Signal router and registers the command

```elixir
defmodule App.Bank.Router do
    use Signal.Router

    register App.Bank.Commands.Deposite

end
```

All dispatched commands are processed in a pipelines.

## Piplines

