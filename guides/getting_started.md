## Getting Started

Note: Signal Applications requires an Event store Module

Creating a Signal Application is as simple as

```elixir
defmodule App.Signal do
    use Signal.Application,
        store: EventStore 
end
```

Then next step is adding the application as part of your 
Application supervision tree

Note: Ensure your Event Store must be started before your application

```elixir
defmodule App.Application do
    use Application

    def start(_, ) do
        children = [
            ...
            App.Signal,
            ...
        ]
        Supervisor.start_link(children, [...])
    end

end
```


