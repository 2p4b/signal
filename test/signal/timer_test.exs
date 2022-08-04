defmodule Signal.TimerTest do

    use ExUnit.Case

    describe "Signal.Timer" do
        test "seconds/1" do
            assert (60 * 1000) == Signal.Timer.seconds(60)
        end
        test "sec/1" do
            assert (60 * 1000) == Signal.Timer.sec(60)
        end

        test "minutes/1" do
            assert (60 * 60 * 1000) == Signal.Timer.minutes(60)
        end

        test "min/1" do
            assert (60 * 60 * 1000) == Signal.Timer.min(60)
        end

        test "hours/1" do
            assert (12 * 60 * 60 * 1000) == Signal.Timer.hours(12)
        end

        test "hrs/1" do
            assert (12 * 60 * 60 * 1000) == Signal.Timer.hrs(12)
        end

        test "days/1" do
            assert (60 * 24 * 60 * 60 * 1000) == Signal.Timer.days(60)
        end
    end

end

