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
        topics: [Deposited, Widthdrawn, "App.Bank.Events.Credited", ...]

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

### Definition

The `use Signal.Handler` Keyword list options

```elixir
    use Signal.Handler, [...options]
```

- `:app` Signal application module
- `:name` handler name if none it given the Module name will be used as process name
- `:start` starts processing events from current cursor position, `:beginning` starts processing events from event number 0
- `:topics` list, events to listern, event name can be string or atom
- `:timeout` number of microseconds of inactivity before handler hibernation
- `:shutdown`  how to shut down a handler, either immediately or by giving it time to shut down
- `:restart` when the handler should be restarted, defaults to :transient


### Callbacks

#### handle_event/2
Invoked to handle all events

#### handle_info/2
[GenServer.handle_info/2](https://hexdocs.pm/elixir/1.12/GenServer.html#c:handle_info/2)

#### handle_call/3
[GenServer.handle_call/3](https://hexdocs.pm/elixir/1.12/GenServer.html#c:handle_call/3)

#### handle_continue/2
[GenServer.handle_continue/2](https://hexdocs.pm/elixir/1.12/GenServer.html#c:handle_continue/2)

#### handle_cast/2
[GenServer.handle_cast/2](https://hexdocs.pm/elixir/1.12/GenServer.html#c:handle_cast/2)
