defmodule Signal.TelemetryTest do
    alias Signal.Telemetry
    use ExUnit.Case
    
    # @TOD: Add test for listerning to signal telemetry events
    describe "Signal.Telemetry" do

        test "path/1" do
            assert Telemetry.path(Signal.Stream.Producer) == [:signal, :stream, :producer]
        end

        test "start/3" do
            signal = Telemetry.path(Signal.Stream.Producer)
            metadata = %{custom_metadata: "test"}
            measurements = %{my_custom_measurement: 10}
            assert is_number(Telemetry.start(signal, metadata, measurements))
        end

        test "stop/4" do
            signal = Telemetry.path(Signal.Stream.Producer)
            metadata = %{custom_metadata: "test"}
            measurements = %{my_custom_measurement: 10}
            start = Telemetry.start(signal, metadata, measurements)
            duration = Telemetry.stop(signal, start, metadata, measurements)
            assert (duration > 0)
        end

    end

end


