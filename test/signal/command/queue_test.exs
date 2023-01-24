defmodule Signal.Command.QueueTest do

    use ExUnit.Case, async: true

    alias Signal.Void.Store

    defmodule TestApplication do
        use Signal.Application,
            store: Store

        queue default: [timeout: :infinity]
        queue :thread, timeout: 500

    end

    setup_all do
        {:ok, _pid} = start_supervised(Store)
        {:ok, _pid} = start_supervised({TestApplication, [name: :queue]})
        :ok
    end

    describe "Queue" do

        @tag :qtimeout
        test "should have timeout options" do
            assert 500 == TestApplication.queue(:thread) |> Keyword.get(:timeout)
            assert :infinity == TestApplication.queue(:default) |> Keyword.get(:timeout)
        end

        @tag :prepare_queue
        test "prepare_queue/3" do

            id = "thread"
            type = :thread

            queue =
                {TestApplication, :queue}
                |> Signal.Execution.Supervisor.prepare_queue(id, type)

            assert is_tuple(queue)
            assert tuple_size(queue) == 3
            assert elem(queue, 2) |> elem(1) == id
            assert elem(queue, 2) |> elem(2) == type
        end

    end

end
