defmodule Signal.Pipe do
    @type ctask :: Signal.Task.t()
    @callback pipe(task :: ctask) :: {:ok, ctask} | {:error, term()}
end
