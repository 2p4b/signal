defmodule Signal.TestCase do
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

            # assert that the expected events are returned when the given commands
            # have been executed
            defp assert_events(command, expected_events) 
            when is_struct(command) and is_struct(expected_events) do
                assert_events(command, [expected_events])
            end

            defp assert_events(command, expected_events) 
            when is_struct(command) and is_list(expected_events) do
                events =
                    case handle(command) do

                        events when is_list(events) ->
                            events

                        event when is_struct(event) ->
                            [event]

                        error ->
                            error

                    end
                if is_list(events) do
                    expected_events
                    |> Enum.with_index()
                    |> Enum.each(fn {event, index} -> 
                        expected = Enum.at(events, index)
                        if expected do 
                            assert event == expected
                        else
                            assert match?(event, nil), """
                            Expected #{inspect(event)}
                            but got none
                            """
                        end
                    end)
                else
                    events
                end
            end

            # execute one or more command
            defp execute(command, params \\ %{}) do
                Signal.Command.Executor.execute(command, params)
            end

            # handle a command
            defp handle(command) when is_struct(command) do
                handle(command, nil, %{})
            end

            defp handle(command, aggregate) 
            when is_struct(command) and is_struct(aggregate) do
                handle(command, aggregate, %{})
            end

            defp handle(command, params) 
            when is_struct(command) and is_map(params) do
                handle(command, nil, %{})
            end

            defp handle(command, aggregate, params \\ %{})
            when is_struct(command) and (is_struct(aggregate) or is_nil(aggregate)) do

                {aggregate_module, _sid} = Signal.Stream.stream(command)

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
            defp evolve(aggregate, event, from \\ 0)

            defp evolve(aggregate, event, from) when is_atom(aggregate) do
                evolve(struct(aggregate, []), event, from)
            end

            defp evolve(aggregate, event, from) when is_struct(event) do
                evolve(aggregate, [event], from)
            end

            defp evolve(aggregate, events, from) when is_list(events) do
                {aggregate, _version} =
                    Enum.reduce(events, {aggregate, from}, fn event, {aggregate, index} -> 

                        unless reduces?(aggregate, event) do
                            {aggregate_module, _sid} = Signal.Stream.stream(event)
                            raise ArgumentError, message: """
                                Event requires aggregate of type 
                                #{inspect(aggregate_module)}
                                #{inspect(event)} 
                            """
                        end

                        version = index + 1
                        meta =  struct(Signal.Events.Event.Meta, [number: version])
                        {Signal.Stream.Reducer.apply(aggregate, meta, event), version}
                    end)
                aggregate
            end

            # check if aggerate reduces an event or command
            defp reduces?(aggregate, payload) 
            when is_atom(aggregate) and is_struct(payload) do
                {aggregate_module, _sid} = Signal.Stream.stream(payload)
                if aggregate_module != aggregate do
                    false
                else
                    true
                end
            end

            defp reduces?(aggregate, payload) 
            when is_struct(aggregate) and is_struct(payload) do
                {aggregate_module, _sid} = Signal.Stream.stream(payload)
                if aggregate_module != aggregate.__struct__ do
                    false
                else
                    true
                end
            end

        end
    end
end
