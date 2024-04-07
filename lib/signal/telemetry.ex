defmodule Signal.Telemety do
    @prefix :signal
    @start :start
    @stop :stop

    def start(signal, metadata \\ %{}, measurements \\ %{}) do
        measurements = add_telemetry_measurements(measurements)
        signal
        |> wrap(@start)
        |> :telemetry.execute(measurements, metadata)
        measurements.systime
    end

    def stop(signal, clock, metadata \\ %{},  measurements \\ %{}) do
        measurements = add_telemetry_measurements(measurements) |> compute_duration(clock)
        signal
        |> wrap(@stop)
        |> :telemetry.execute(measurements, metadata)
    end

    defp wrap(signal, path) when is_atom(signal) and is_list(path) do
        List.wrap(@prefix) ++ List.wrap(path) ++ List.wrap(signal)
    end

    defp compute_duration(%{systime: systime}=measurements, clock) do
        duration = systime - clock
        Map.put(measurements, :duration, duration)
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

end
