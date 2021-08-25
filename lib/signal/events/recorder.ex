defmodule Signal.Events.Recorder do

    use GenServer

    alias Signal.Stream.Event
    alias Signal.Events.Staged
    alias Signal.Events.Record
    alias Signal.Stream.History
    alias Signal.Command.Action
    alias Signal.Events.Producer
    alias Signal.Events.Recorder

    defstruct [:cursor, :app, :store]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        params = [name: Keyword.get(opts, :app) |> name()]
        GenServer.start_link(__MODULE__, opts, params)
    end

    def name({module, app_name}) do
        if module == app_name do
            Module.concat([module, Recorder])
        else
            Module.concat([module, Recorder, app_name])
        end
    end

    @impl true
    def init(opts) do
        app = Keyword.get(opts, :app)
        store = Keyword.get(opts, :store)
        cursor =  store.cursor(app)
        {:ok, struct(__MODULE__, [app: app, store: store, cursor: cursor])}
    end

    @impl true
    def handle_call(:cursor, _from, %Recorder{cursor: cursor}=state) do
        {:reply, cursor, state}
    end

    @impl true
    def handle_call({:record, action, staged}, _from, %Recorder{}=state) 
    when is_list(staged) do

        %Recorder{store: store, app: app} = state

        {histories, {events, indices, position}} =
            staged
            |> Enum.map_reduce({[], [], state.cursor}, fn stage, {acc_events, indices, index} -> 

                %Staged{stream: stream, version: version, events: events} = stage

                {indexed_events, index} =
                    events
                    |> Enum.map_reduce(index, &Recorder.index_event/2)

                history = %History{
                    stream: stream, 
                    version: version, 
                    events: indexed_events
                }

                indices = indices ++ [{Producer.stream_id(stream), version}]

                {history, {acc_events ++ indexed_events, indices, index}}
            end)

        log = %Signal.Log{
            cursor: position,
            states: action.states,
            params: action.params,
            result: action.result,
            streams: histories,
            command: action.command,
            indices: action.indices ++ indices,
        }

        with {:ok, cursor} <- store.record(app, log) do
            Enum.each(events, &(Signal.Application.publish(app, &1)))
            record = %Record{index: cursor,  histories: histories}
            state = %Recorder{ state | cursor: cursor}
            {:reply, record, state}
        else
            error ->
                {:reply, error, state}
        end
    end

    def index_event(%Event{}=event, cursor) do
        number = cursor + 1
        {%Event{event | number: number}, number}
    end

    def cursor(app) do
        Recorder.name(app)
        |> GenServer.call(:cursor)
    end

    def record(app, %Action{}=action, staged) when is_list(staged) do
        Recorder.name(app)
        |> GenServer.call({:record, action, staged})
    end

    def record(app, action, staged) do
        record(app, action, [staged])
    end

end
