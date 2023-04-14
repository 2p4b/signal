# Signal

Use Signal to build an Event Driven Elixir application

Supports:
- Command Dispatching
- Stream Event reduction with Aggregates
- Event Handlers with atleast once event delivery
- Saga Process

Signal makes heavy use of the [Blueprint](https://github.com/mfoncho/blueprint.git)
for struct definition, validation, encoding and decoding

## Installation

```elixir
def deps do
    [
        {:signal, git: "https://github.com/mfoncho/signal.git"}
    ]
end
```

### Guides
- [Getting started](guides/getting_started.md)
- [Commands](guides/commands.md)

