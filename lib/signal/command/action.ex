defmodule Signal.Command.Action do

    alias Signal.Task
    alias Signal.Command.Action

    defstruct [
        :app,
        :params,
        :result,
        :stream,
        :command,
        :indices,
        :snapshots,
        :consistent,
        :causation_id,
        :correlation_id,
    ]

    def from(%Task{command: command, app: app, assigns: params}=task) do

        stream = Signal.Stream.stream(command)

        struct(Action, [
            app: app,
            params: params,
            stream: stream,
            result: task.result,
            command: command,
            indices: task.indices,
            snapshots: task.snapshots,
            consistent: task.consistent,
            causation_id: task.causation_id, 
            correlation_id: task.correlation_id
        ])
    end

end
