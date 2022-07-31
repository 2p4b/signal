defmodule Signal.ApplicationTest do
    use ExUnit.Case, async: true

    alias Signal.Task

    defmodule Command do
        use Signal.Command
        schema do
            field :any, :string, default: nil
        end
    end

    defmodule PipeOne do
        import Signal.Task
        def handle(task) do
            {:ok, assign(task, :one, 1)}
        end
    end

    defmodule PipeTwo do
        import Signal.Task
        def handle(task) do
            {:ok, assign(task, :two, 2)}
        end
    end

    defmodule Router do

        use Signal.Router

        register Command

    end

    defmodule App do

        use Signal.Application,
            store: Signal.Void.Store

        pipe :pipe_one, PipeOne
        pipe :pipe_two, PipeTwo

        pipeline :combined do
            via :pipe_one
            via :pipe_two
        end

        router Router, via: :combined

    end

    setup do
        {:ok, _pid} = start_supervised(App)
        :ok
    end

    describe "Application test" do

        @tag :application
        test "Yes the app should at least start with children" do
            %{} = Supervisor.count_children(App)
        end

        @tag :application
        test "should have two pipes" do
            task = Task.new(Command.new([]), [])
            assert {:ok, %Task{assigns: %{one: 1}}} = App.pipe_one(task)
            assert {:ok, %Task{assigns: %{two: 2}}} = App.pipe_two(task)
        end

        @tag :application
        test "should have pipeline which applies two pipes" do
            task = Task.new(Command.new([]), [])
            assert {:ok, %Task{assigns: %{one: 1, two: 2}}} = App.combined(task)
        end

        @tag :application
        test "application should be able to router Command with pipeline options" do
            assert {Router, opts}  = Command.new([]) |> App.handler()
            assert [{App, :combined}] = Keyword.get(opts, :via)
        end

    end

end
