## Commands

The purpose of a command is to generate events

```elixir
defmodule App.Bank.Commands.Deposite do
    alias App.Bank.Account
    alias App.Bank.Events.Deposited
    alias App.Bank.Commands.Deposite

    # [optional] :sync is optional, default: false
    # [optional] :queue is optional, default: nil
    # [optional] :name is optional, default: [module name]
    # [required] :stream
    use Signal.Command,
        sync: true,
        name: "App.Bank.Commands.Deposite",
        queue: :account_id,
        stream: {:account_id, Account}

    schema [required: true] do
        field: :account_id, :uuid
        field: :amount,     :float
        field: :timstamp,   :datetime
    end

    # [optional] Executes command
    def execute(%Deposite{}=deposite, pipeline_params) do
        # Database access
        # Http remote resource access
        # {:error, reason} stop command processing and return error to dispatcher
        {:ok, execution_result}
    end

    # Handle the command and return events or error tuple
    def handle(%Deposite{}=deposite, execution_result, %Account{}=account) do
        # {:error, reason} stop command processing and return error to dispatcher
        %Deposited{...}
    end

end
```

### Definition

The `use Signal.Command` Keyword list options

```elixir
    use Signal.Command, [...options]
```
- `:sync` [optional] sync stream aggregate
- `:name` [optional] command name, default to module name
- `:queue` [optional] execution queue id
- `:stream` [required] command event stream tag


### callbacks

#### execution queue execute/2
The `execute` callback is executed before the `handle` callback.
If a queue is defined the command will be executed in the specified queue.
In the sample `App.Bank.Commands.Deposite` each command is executed in a process
queue with the id of the `:account_id` if no queue is defined or is `nil` the command
will be executed on the dispatch caller process

#### command handle/3
The `handle` callback handles the command, after it's executed and sent
to the stream (`{Account, :account_id}`) producer to generated event(s)

The `handle` callback accepts three arguements the 
- The command being handled `%Deposite{}`
- The results of the command execution if `execute` callback is defined or the pipline params
- The command Stream Aggregate `%Account{}`

#### command stream syncronization
The `sync: true` option is used to specify if the command requires the most recent Stream aggregate version.
while `sync: false` will use the available stream aggregate version without any gaurantee all available stream events have been applied to the aggregate, this might be a desired option when the most recent aggregate state is not important in event creation and or performance is a priority


### Dispatch

Once the command has been registered in a [router](router.md) the command can be dispatched using the signal application

```elixir
    alias Signal.Result
    alias App.Bank.Commands.Deposite

    with {:ok, %Result{}} <- App.Signal.dispatch(%Deposite{}) do
        ...
    end
```
