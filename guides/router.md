# Router

Routers are used to register commands and processing pipelines

This router defines a Signal router and registers the command

```elixir
defmodule App.Bank.Router do
    use Signal.Router

    register App.Bank.Commands.Deposite, 
        # [optional] :await 
        # wait for command stream events to be applied
        # applied to aggregates before return 
        # to dispatcher with aggregate states
        await: true

end
```


## Piplines/Middleware

### terminology
**pipe**: A pip is a stage in execution of a command task
**pipeline**: A pipeline is a collection of pipes and/or pipelines


All dispatched commands are processed in a pipes.

Pipes and pipelines can be inlined in routers and used only within the router
in which they are defined, or pipes can be a module and used in 
multiple routers

Each pipe stage can assign values to the command task which in turn will be
available to the next pipe stage and the last pipe assigned
Map will be used in the `execute` command callback or passed 
as the execution result of the command when handling `handle`

Each stage must return an `{:ok, %Signal.Task{...}}` tuple to continue
to the next pipe stage or`{:error, reason}` to stop processing the 
command and return the error tuple to the dispatcher

Signal.Task has and `assign` method to assign fields to the command
task as its goes though each pipe in the pipeline

Each command type is assigned a pipe or pipeline to pass the command task
through using the `via: :pipe_or_pipeline_name`, option

#### inline pipe and pipeline

```elixir
defmodule App.Bank.Router do
    use Signal.Router
    import Signal.Task

    pipe :inline_pipe1, fn task -> 
        {:ok, assign(task, :pipe1, true)}
    end 

    pipe :inline_pipe2, fn task -> 
        {:ok, assign(task, :pipe2, true)}
    end 

    # inline_pipe1 will be executed before inline_pipe2
    pipeline :pipeline1 do
        via :inline_pipe1
        via :inline_pipe2
    end

    # inline_pipe2 will be executed before inline_pipe1
    pipeline :pipeline2 do
        via :inline_pipe2
        via :inline_pipe1
    end

    register App.Bank.Commands.Deposite, via: :pipeline2

    register App.Bank.Commands.Widthdraw, via: :inline_pipe1

end
```

#### Module based pipe

```elixir
defmodule Validate do
    import Signal.Task
    @behaviour Signal.Pipe

    def pipe(task) do
        {:ok, assign(task, :validated, true)}
    end
end

# Pipe usage
defmodule App.Bank.Router do
    use Signal.Router
    import Signal.Task

    pipe :inline_pipe1, fn task -> 
        {:ok, assign(task, :pipe1, true)}
    end 

    #add Validate pipe
    pipe :validate, Validate

    # :validate will be executed before inline_pipe1
    pipeline :pipeline1 do
        via :validate
        via :inline_pipe1
    end

    register App.Bank.Commands.Deposite, via: :pipeline1

    register App.Bank.Commands.Widthdraw, via: :validate

end
```
