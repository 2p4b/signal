defmodule Signal.Events.Record do

    defstruct [:index, :histories]

    alias Signal.Stream.History

    def stream_version(%__MODULE__{histories: histories}, stream, default \\ nil) do
        histories
        |> Enum.find(&(Map.get(&1, :stream) == stream)) 
        |> (fn 
            %History{stream: ^stream, version: version} -> 
                version
            nil -> 
                default
        end).()
    end

    def histories(%__MODULE__{histories: histories}) do
        histories
    end

end
