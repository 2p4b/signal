# Signal

Use Signal to build an Event Driven Elixir application

Supports:
- Command Dispatching
- Stream Event reduction with Aggregates
- Event Handlers with atleast once event delivery
- Saga Process

Signal makes heavy use of the [Blueprint](https://github.com/2p4b/blueprint.git)
for struct definition, validation, encoding and decoding

## Installation

```elixir
def deps do
    [
        {:signal, git: "https://github.com/2p4b/signal.git"}
    ]
end
```

### Guides
- [Getting started](guides/getting_started.md)
- [Commands](guides/commands.md)
- [Events](guides/event.md)
    - [Event Handler](guides/handler.md)
    - [Event Projector](guides/projector.md)
- [Router](guides/router.md)
- [Aggregate](guides/aggregate.md)
- [Processes and Sagas](guides/process.md)

