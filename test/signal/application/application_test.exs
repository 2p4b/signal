defmodule Signal.ApplicationTest do
    use ExUnit.Case, async: true

    defmodule App do
        use Signal.Application,
            store: Signal.VoidStore
    end

    setup do
        {:ok, _pid} = start_supervised(App)
        :ok
    end
    
    describe "Application test" do

        @tag :application
        test "should have two child supervisors" do
            %{} = Supervisor.count_children(App)
            #assert children.active == 11
            #assert children.supervisors == 10
        end

    end

end
