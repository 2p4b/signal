defmodule Signal.ApplicationTest do
    use ExUnit.Case, async: true

    defmodule App do
        use Signal.Application,
            store: Signal.Void.Store
    end

    setup do
        {:ok, _pid} = start_supervised(App)
        :ok
    end

    describe "Application test" do

        @tag :application
        test "should have two child supervisors" do
            %{} = Supervisor.count_children(App)
        end

    end

end
