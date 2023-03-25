defmodule Signal.Command.RouterTest do
    use ExUnit.Case

    alias Signal.Task

    describe "router test" do

        defmodule PipeCommand do
            use Signal.Command
            schema do
                field :one, :number
                field :two, :number
            end
        end

        defmodule PipelineCommand do
            use Signal.Command

            schema do
                field :uuid,        :string
                field :one,         :number
                field :two,         :number
                field :executed,    :number
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

        defmodule EmptyRouter do
            use Signal.Router
        end

        defmodule Router do
            use Signal.Router
            import Signal.Task

            pipe :pipe_one, PipeOne
            pipe :pipe_two, PipeTwo

            pipe :fn_pipe, fn task -> 
                {:ok, assign(task, :fn_pipe, true)}
            end 

            pipeline :pipeline do
                via :pipe_one
                via :pipe_two
                via :fn_pipe
            end

            register PipeCommand,
                await: true,
                via: :pipe_one,
                consistent: true

            register PipelineCommand, via: :pipeline
        end


        @tag :router
        test "should pass through pipe only" do
            assert {:ok, %{assigns: %{one: 1}} } =
                Router.run(PipeCommand.new(),[])
        end

        @tag :router
        test "should pass through pipeline" do
            assert {:ok, %{assigns: %{one: 1, two: 2, fn_pipe: true}} } =
                Router.run(PipelineCommand.new(),[])
        end

        @tag :router
        test "should register pipeline" do
            assert {:ok, _task} =
                Router.run(PipelineCommand.new(),[])
        end

        @tag :router
        test "can apply side effect to command" do
            case Router.run(PipelineCommand.new(), []) do
                {:ok, %Task{}} ->
                    assert true
                _ ->
                    assert false
            end
        end

    end

end
