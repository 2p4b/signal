## Handler

An Event handler is a process that handles events from different streams
and with its own state lifecycle and behaviour like any other GenServer process 
with an addition of a new callback behaviour `handle_event` with same return
values as `GenServer.handle_info`

```elixir
defmodule App.Bank.TransactionLogger do
    alias App.Bank.Events.Deposited
    alias App.Bank.Events.Widthdrawn
    use Signal.Handler,
        app: App.Signal,
        topics: [Deposited, Widthdrawn, ...]

    def init(opts) do
        {:ok, state}
    end

    # return same value as GenServer.handle_info
    def handle_event(%Deposited{}, state) do
        {:noreply, state}
    end


    # Same callbacks as GenServer
    def handle_info(event, state) do
        {:noreply, state}
    end

    def handle_call(args, from, state) do
        {:noreply, state}
    end

    ...

end
```
