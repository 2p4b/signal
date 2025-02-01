defmodule Signal.HelperTest do

    use ExUnit.Case
    alias Signal.Helper

    defmodule Sample.Command do
        use Signal.Command,
            stream: {:parent_id, StreamType}

        schema required: true do
            field :parent_id, :uuid
        end

    end

    describe "Signal.Helper" do

        test "Signal.Helper.stream_id for sub stream object default hash" do
            root_id = UUID.uuid1()
            stream_obj = %Sample.Command{parent_id: root_id}
            {stream_id, stream_type} = Signal.Stream.stream(stream_obj)
            assert stream_id == Helper.stream_id({nil, root_id})
            assert stream_type == StreamType
        end

        test "Signal.Helper.atom_to_string/1" do
            assert "hello" == Signal.Helper.atom_to_string(:hello)
            assert "Signal.HelperTest.Sample.Command" == Signal.Helper.atom_to_string(Sample.Command)
        end


    end
end
