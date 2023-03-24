defmodule Signal.HelperTest do

    use ExUnit.Case
    alias Signal.Helper

    describe "Signal.Helper" do
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
