defmodule Signal.Probe do
    @moduledoc """
    This module defines the test case to be used by tests.
    """
    use ExUnit.CaseTemplate

    using do
        quote do

            # assert command or event stream
            defp assert_stream(stream, payload) do
                assert stream = Signal.Stream.stream(payload)
            end

            defp assert_emitted(event, events) 
            when is_struct(event) and is_list(events) do
                case Enum.find(events, &match?(&1, event)) do
                    nil ->
                        assert false, """
                        Event #{inspect(event)}
                        not found
                        """
                    _ ->
                        {:ok, event}
                end
            end

            # assert that the expected events are returned when the given commands
            # have been executed
            defp assert_events(command, expected_events) 
            when is_struct(command) and is_struct(expected_events) do
                assert_events(command, [expected_events])
            end

            defp assert_events(command, expected_events) 
            when is_struct(command) and is_list(expected_events) do
                events =
                    case handle_command(command) do
                        nil ->
                            []

                        {:ok, events} when is_list(events) ->
                            events

                        {:ok, event} when is_struct(event) ->
                            [event]

                        events when is_list(events) ->
                            events

                        event when is_struct(event) ->
                            [event]

                        %Signal.Multi{}=multi ->
                            Signal.Multi.emit(multi)

                        error ->
                            error
                    end

                if is_list(events) do
                    expected_events
                    |> Enum.each(fn event -> 
                        assert_emitted(event, events)
                    end)
                else
                    message  = """
                    #{inspect(events)}

                    failed to handle command
                    #{inspect(command)}
                    """
                    stream = Signal.Stream.stream(command)
                    raise(Signal.Exception.StreamError, [stream: stream, message: message])
                end
            end

            # execute one or more command
            defp execute(command, params \\ %{}) do
                Signal.Command.Handler.execute(command, params)
            end

            # handle a command
            defp handle_command(command) when is_struct(command) do
                handle_command(command, nil, %{})
            end

            defp handle_command(command, aggregate) 
            when is_struct(command) and is_struct(aggregate) do
                handle_command(command, aggregate, %{})
            end

            defp handle_command(command, params) 
            when is_struct(command) and is_map(params) do
                handle_command(command, nil, %{})
            end

            defp handle_command(command, aggregate, params \\ %{})
            when is_struct(command) and (is_struct(aggregate) or is_nil(aggregate)) do

                {_sid, aggregate_module} = Signal.Stream.stream(command)

                case execute(command, params) do
                    {:error, reason} = error ->
                        error

                    :error ->
                        {:error, :execution_error}

                    result ->
                        result =
                            case result do
                                {:ok, result} -> 
                                    result
                                result -> 
                                    result
                            end

                        aggregate =
                            if is_nil(aggregate) do
                                struct(aggregate_module, [])
                            else
                                unless reduces?(aggregate, command) do
                                    raise ArgumentError, message: """
                                        Command requires aggregate of type
                                        #{inspect(aggregate_module)}
                                        #{inspect(command)} 
                                    """
                                end
                                aggregate
                            end
                        Signal.Command.Handler.handle(command, result, aggregate)
                end
            end

            # apply the given events to the aggregate state
            defp evolve(aggregate, event)

            defp evolve(aggregate, event) when is_atom(aggregate) do
                evolve(struct(aggregate, []), event)
            end

            defp evolve(aggregate, event) when is_struct(event) do
                evolve(aggregate, [event])
            end

            defp evolve(aggregate, events) when is_list(events) do
                Enum.reduce(events, aggregate, fn event, aggregate -> 

                    unless reduces?(aggregate, event) do
                        {_sid, aggregate_module} = Signal.Stream.stream(event)
                        raise ArgumentError, message: """
                            Event requires aggregate of type 
                            #{inspect(aggregate_module)}
                            #{inspect(event)} 
                        """
                    end

                    case Signal.Stream.Reducer.apply(aggregate, event) do
                        {:ok, state}  ->
                            state

                        {:ok, state, timeout} when is_number(timeout)  ->
                            state

                        {:snapshot, state} ->
                            state

                        {:snapshot, state, :sleep} ->
                            state

                        {:snapshot, state, timeout} when is_number(timeout) ->
                            state

                        {:sleep, state} ->
                            state

                        {:sleep, state, timeout} when is_number(timeout) ->
                            state

                        {:error, error} ->
                            raise RuntimeError, message: """
                                Error reducing event
                                #{inspect(error)}
                                Aggregte:
                                    #{inspect(aggregate)}
                                    
                                Event:
                                    #{inspect(event)} 
                            """
                    end
                end)
            end

            # check if aggerate reduces an event or command
            defp reduces?(aggregate, payload) 
            when is_atom(aggregate) and is_struct(payload) do
                {_sid, aggregate_module} = Signal.Stream.stream(payload)
                if aggregate_module != aggregate do
                    false
                else
                    true
                end
            end

            defp reduces?(aggregate, payload) 
            when is_struct(aggregate) and is_struct(payload) do
                {_sid, aggregate_module} = Signal.Stream.stream(payload)
                if aggregate_module != aggregate.__struct__ do
                    false
                else
                    true
                end
            end

        end
    end
end
