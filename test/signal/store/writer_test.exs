defmodule Signal.Store.WriterTest do
    use ExUnit.Case, async: true
    import Signal.WriterFactory

    alias Signal.Transaction
    alias Signal.Store.Writer


    describe "Writer" do
        @tag :prepare_transaction
        test "prepare_transaction/2" do
            number_of_streams = 10
            events_per_stream = 4
            staged = generage_staged(number_of_streams, events_per_stream)

            writer = build(:writer)
            pre_transaction = build(:transaction, [staged: staged])

            transaction = 
                writer
                |> Writer.prepare_transaction(pre_transaction)
            assert match?(%Transaction{}, transaction)
            assert transaction.cursor === ((number_of_streams * events_per_stream) + writer.index)
        end
    end

    defp generage_staged(number_of_streams, events_per_stream) 
    when is_integer(number_of_streams) and  number_of_streams > 0 and is_integer(events_per_stream) and events_per_stream > 0 do
        Range.new(1, number_of_streams)
        |> Enum.map(fn val -> String.Chars.to_string(val) end)
        |> Enum.map(fn stream_id -> 
              [stream_id: stream_id]
              |> create_stream_event()
              |> stage_event_multi(events_per_stream)
        end)
    end

    def stage_event_multi(event, factor \\ 2) 
    when is_integer(factor) and factor > 0 do
        events = 
            Range.new(1, factor)
            |> Enum.map(fn index ->  
                Signal.Stream.Event.new(event, [index: index])
            end)
        %Signal.Stream.Stage{
            stream: Signal.Stream.stream(event),
            events: events,
            position: factor,
            stage: self(),
        }
    end

    def create_stream_event(opts) do
        Signal.Sample.Event.new(opts)
    end

end

