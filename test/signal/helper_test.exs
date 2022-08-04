defmodule Signal.HelperTest do

    use ExUnit.Case

    describe "Signal.Helper" do
        test "stream_tupe/3" do
            assert {"stream_id", __MODULE__} ==
                __MODULE__
                |> Signal.Helper.stream_tuple("stream_id", [])
        end

        test "stream_tupe/3 with prefix" do
            assert {"prefix:stream_id", __MODULE__} ==
                __MODULE__
                |> Signal.Helper.stream_tuple("stream_id", [prefix: "prefix:"])
        end

        test "module_to_string/1" do
            assert "Signal.HelperTest" ==
                __MODULE__
                |> Signal.Helper.module_to_string()
        end

        test "string_to_module/1" do
            assert __MODULE__ ==
                Signal.Helper.string_to_module("Signal.HelperTest")
        end
    end
end
