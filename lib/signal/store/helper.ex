defmodule Signal.Store.Helper do

    @min_number 1

    def event_is_valid?(_event, [], []) do
        true
    end

    def event_is_valid?(event, streams, []) when is_list(streams) do
        event.stream_id in streams
    end

    def event_is_valid?(event, [], topics) when is_list(topics) do
        event.topic in topics
    end

    def event_is_valid?(event, streams, topics) 
    when is_list(topics) and is_list(streams) do
        event_is_valid?(event, streams, []) and event_is_valid?(event, [], topics)
    end

    def range([]) do 
        stream_direction(@min_number, :max)
    end

    def range([first]) when is_integer(first) do 
        stream_direction(first, :max)
    end

    def range(%Range{first: first, last: last}) do 
        stream_direction(first, last)
    end

    def range([first, last]) when is_integer(first) and is_integer(last) do 
        stream_direction(first, last)
    end

    defp stream_direction(first, :max) when is_integer(first) do
        [first, :max, :asc]
    end

    defp stream_direction(:min, last) when is_integer(last) do
        [@min_number, last, :desc]
    end

    defp stream_direction(first, last)
    when is_integer(first) and is_integer(last) and first >= 0 and last >= 0 do
        if first > last do
            [last, first, :desc]
        else
            [first, last, :asc]
        end
    end

end
