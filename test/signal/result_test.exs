defmodule Signal.ResultTest do
    alias Signal.Result

    use ExUnit.Case

    defmodule Aggregate do
        use Signal.Aggregate
        schema do
            field :aggregate_id, :string
        end
    end

    defmodule Event do
        use Signal.Event,
            stream: {Aggregate, :aggregate_id}

        schema do
            field :aggregate_id, :string
        end
    end

    setup do
        event =  %Event{aggregate_id: "aggregate.id"}
        aggregate =  %Aggregate{aggregate_id: "aggregate.id"}
        assigns = %{one: 1, two: 2}

        result = %Result{
            index: 3, 
            assigns: assigns,
            result: %{"one" => 1, "two" => 2},
            states: [aggregate],
            events: [event]
        }

        {:ok, %{result: result, states: [aggregate], events: [event], assigns: assigns}}
    end

    describe "Signal.Result" do
        test "Signal.Result.ok/1" do
            value = 1
            assert {:ok, ^value} = Result.ok(value)
        end
        test "Signal.Result.error/1" do
            value = 1
            assert {:error, ^value} = Result.error(value)
        end

        test "Signal.Result.ok?/1" do
            value = 1
            assert Result.ok?({:ok, value})
            refute Result.ok?({:error, value})
        end

        test "Signal.Result.error?/1" do
            value = 1
            assert Result.error?({:error, value})
            refute Result.error?({:ok, value})
        end

        test "Signal.Result.assigned/1", %{result: result} do
            assert match?(1, Result.assigned(result, :one))
            assert match?(2, Result.assigned(result, :two))
        end

        test "Signal.Result.assigns/1", %{result: result, assigns: assigns} do
            assert match?(^assigns, Result.assigns(result))
        end

        test "Signal.Result.events/1", %{result: result, events: events} do
            assert match?(^events, Result.events(result))
        end

        test "Signal.Result.states/1", %{states: states, result: result} do
            assert match?(^states, Result.states(result))
        end

    end

end


