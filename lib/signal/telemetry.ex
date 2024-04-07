defmodule Signal.Telemetry do
    @start :start
    @stop :stop

    def start(signal, metadata \\ %{}, measurements \\ %{}) when is_list(signal) do
        measurements = add_telemetry_measurements(measurements)
        signal
        |> prepend_action(@start)
        |> :telemetry.execute(measurements, metadata)
        measurements.systime
    end

    def stop(signal, clock, metadata \\ %{},  measurements \\ %{}) when is_list(signal) do
        measurements = add_telemetry_measurements(measurements) |> compute_duration(clock)
        signal
        |> prepend_action(@stop)
        |> :telemetry.execute(measurements, metadata)
        measurements.duration
    end

    # Convert a module to a list of atoms
    # representing the module telemetry path
    # Example: Signal.Stream.Producer -> [:signal, :stream, :producer]
    def path(module) when is_atom(module) do
        module
        |> Module.split()
        |> Enum.map(&(String.downcase(&1) |> String.to_atom()))
    end

    defp prepend_action(path, signal) when is_atom(signal) and is_list(path) when is_list(path) do
        List.wrap(path) ++ List.wrap(signal)
    end

    defp compute_duration(%{systime: systime}=measurements, clock) do
        duration = systime - clock
        Map.put(measurements, :duration, duration)
    end

    defmacro telemetry_start(signal, path) do
        quote do
            Signal.Telemetry.start(unquote(signal), unquote(path))
        end
    end

    # Add common telemetry metadata
    defp add_telemetry_measurements(measurements) do
        clock = System.monotonic_time()
        systime = System.system_time()
        measurements
        |> Map.put(:pid, self())
        |> Map.put(:clock, clock)
        |> Map.put(:systime, systime)
    end

    defmacro __using__(_) do
        quote location: :keep do
            require Signal.Telemetry
        end
    end

end
