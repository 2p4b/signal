## Commands

The purpose of a command is to generate events

```elixir
defmodule App.Bank.Commands.Deposite do
    alias App.Bank.Account
    alias App.Bank.Events.Deposited
    alias App.Bank.Commands.Deposite

    # [optional] :sync is optional, default: false
    # [optional] :queue is optional, default: nil
    # [required] :stream
    use Signal.Command,
        sync: true,
        queue: :account_id,
        stream: {Account, :account_id}

    schema [required: true] do
        field: :account_id, :uuid
        field: :amount,     :float
        field: :timstamp,   :datetime
    end

    # [optional] Executes events
    def execute(%Deposite{}=deposite, pipeline_params) do
        # Database access
        # Http remote resource access
        {:ok, ...excution_result}
    end

    # Handle the command and return events or error tuple
    def handle(%Deposite{}=deposite, result, %Account{}=account) do
        %Deposited{...}
    end

end
```
#### execution queue
The `execute` callback is executed before the `handle` callback.
If a queue is defined the command will be executed in the specified queue.
In the sample `App.Bank.Commands.Deposite` each command is executed in a process
queue with the id of the `:account_id` if no queue is defined or is `nil` the command
will be executed on the dispatch caller process

#### command handle
The `handle` callback handles the command, after it's executed and sent
to the stream (`{Account, :account_id}`) producer to generated event(s)

The `handle` callback accepts three arguements the 
- 1) The command being handled `%Deposite{}`
- 2) The results of the command execution if `execute` callback is defined or the pipline params
- 3) The command Stream Aggregate `%Account{}`

The `sync: true` option is used to specify if the command requires the most recent aggregate version 




