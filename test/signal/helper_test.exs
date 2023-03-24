defmodule Signal.HelperTest do

    use ExUnit.Case
    alias Signal.Helper

    defmodule Sample.Command do
        use Signal.Command,
            stream: {StreamType, :parent_id, name: "subname"}

        schema required: true do
            field :parent_id, :uuid
        end

    end

    defmodule Sample.SHA.Command do
        use Signal.Command,
            stream: {StreamType, :parent_id, name: "subname", hash: :sha}

        schema required: true do
            field :parent_id, :uuid
        end
    end

    describe "Signal.Helper" do

        test "Signal.Stream.stream for sub stream object default hash" do
            sub_name = "subname"
            root_id = UUID.uuid1()
            stream_obj = %Sample.Command{parent_id: root_id}
            {sample_stream_id, sample_stream_type} = Signal.Stream.stream(stream_obj)
            opts = [name: sub_name]
            stream_id = Helper.stream_id(root_id, opts)
            assert stream_id !== root_id
            assert stream_id === Helper.namespaced_uuid(root_id, sub_name, :md5)
            assert stream_id === sample_stream_id
            assert StreamType === sample_stream_type
        end

        test "Signal.Stream.stream for sub stream object hash sha" do
            sub_name = "subname"
            root_id = UUID.uuid1()
            stream_obj = %Sample.SHA.Command{parent_id: root_id}
            {sample_stream_id, sample_stream_type} = Signal.Stream.stream(stream_obj)
            opts = [name: sub_name, hash: :sha]
            stream_id = Helper.stream_id(root_id, opts)
            assert stream_id !== root_id
            assert stream_id === Helper.namespaced_uuid(root_id, sub_name, :sha)
            assert stream_id === sample_stream_id
            assert StreamType === sample_stream_type
        end

        test "stream_id/2 default md5" do
            sub_name = "sub"
            root_id = UUID.uuid1()
            opts = [name: sub_name]
            stream_id = Helper.stream_id(root_id, opts)
            assert stream_id !== root_id
            assert stream_id === Helper.namespaced_uuid(root_id, sub_name, :md5)
        end

        test "stream_id/2 :md5" do
            sub_name = "sub"
            root_id = UUID.uuid1()
            opts = [name: sub_name, hash: :md5]
            stream_id = Helper.stream_id(root_id, opts)
            assert stream_id !== root_id
            assert stream_id === Helper.namespaced_uuid(root_id, sub_name, :md5)
        end

        test "stream_id/2 :sha" do
            sub_name = "sub"
            root_id = UUID.uuid1()
            opts = [name: sub_name, hash: :sha]
            stream_id = Helper.stream_id(root_id, opts)
            assert stream_id !== root_id
            assert stream_id === Helper.namespaced_uuid(root_id, sub_name, :sha)
        end

        test "string_to_module/1" do
            assert __MODULE__ ==
                Signal.Helper.string_to_module("Signal.HelperTest")
        end
    end
end
