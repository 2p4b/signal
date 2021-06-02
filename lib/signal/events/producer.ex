defmodule Signal.Events.Producer do
    use GenServer

    alias Signal.Result
    alias Signal.Events
    alias Signal.Events.Event
    alias Signal.Events.Staged
    alias Signal.Events.Record
    alias Signal.Command.Action
    alias Signal.Stream.History
    alias Signal.Events.Recorder
    alias Signal.Events.Producer

    defstruct [:app, :store, :stream, cursor: 0]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        Process.send(self(), :init, [])
        {:ok, struct(__MODULE__, opts)}
    end

    @impl true
    def handle_info(:init, %Producer{}=state) do
        {:noreply, calibrate(state)}
    end

    @impl true
    def handle_call(:cursor, _from, %Producer{cursor: cursor}=state) do
        {:reply, cursor, state}
    end

    @impl true
    def handle_call({:stage, action, events}, from, %Producer{}=state) 
    when is_list(events) do

        %Producer{app: app} = state

        channel = 
            app
            |> Signal.Application.supervisor(Task)
            |> Task.Supervisor.async_nolink(fn -> 
                receive do
                    {:ok, version} ->
                        {:ok, version}

                    {:rollback, reason} ->
                        {:rollback, reason}

                    error ->
                        error
                end
            end)

        stage = stage_events(state, action, events, channel.pid)

        %Staged{version: version} = stage

        GenServer.reply(from, stage)

        # Halt until the task is resolved
        case Task.yield(channel, :infinity) do
            {:ok, {:ok, ^version}} ->
                {:noreply, %Producer{state | cursor: version}}

            {:ok, {:rollback, _}} ->
                {:noreply, calibrate(state)}

             _ ->
                {:noreply, calibrate(state)}
        end
    end

    @impl true
    def handle_call({:process, %Action{}=action}, _from, %Producer{}=state) do

        %{stream: stream, app: app} = state

        task_supervisor = Signal.Application.supervisor(app, Task)

        aggregate = aggregate_state(state, action.consistent)

        events = handle_command(action.command, action.params, aggregate)

        cond do
            is_list(events) ->

                staged =
                    events
                    |> Enum.group_by(fn event -> 
                        case Signal.Stream.stream(event) do
                            {type, id} when is_atom(type) ->
                                {type, id}
                            _ ->
                                nil
                        end
                    end)
                    |> Enum.map(fn
                        {^stream, events} ->
                            Task.Supervisor.async_nolink(task_supervisor, fn -> 
                                stage_events(state, action, events, self())
                            end)


                        {stream, events} ->
                            # Process event steams in parallel
                            Task.Supervisor.async_nolink(task_supervisor, fn -> 
                                app
                                |> Signal.Events.Supervisor.prepare_producer(stream)
                                |> stage_events(action, events)
                            end)

                    end)
                    |> Task.yield_many(:infinity)
                    |> Enum.map(fn {_task, res} -> 
                        case res do
                            {:ok, resp} -> 
                                resp

                            {:exit, reason} -> 
                                {:error, reason}
                        end
                    end)

                failed = Enum.find(staged, &Result.error?/1)

                {reply, state} =

                    if failed do
                        reason = elem(failed, 1)
                        rollback_staged(staged, reason)
                        {failed, state}

                    else
                        case Recorder.record(app, action, staged) do
                            %Record{histories: histories} ->

                                confirm_staged(staged)

                                cursor = 
                                    histories
                                    |> Enum.find(&(Map.get(&1, :stream) == stream)) 
                                    |> (fn 
                                        %History{stream: ^stream, version: cursor} -> 
                                            cursor 
                                        nil -> 
                                            state.cursor
                                    end).()

                                reply = Result.ok(histories)

                                state = %Producer{state | cursor: cursor}

                                {reply, state}

                            error ->
                                reason =
                                    case error do
                                        {:error, reason} -> 
                                            reason
                                        _ -> nil
                                    end

                                rollback_staged(staged, reason)

                                {error, state}
                        end
                    end

                {:reply, reply, state}

            true  ->
                {:reply, events, state}
        end
    end

    def stage_events(producer, action, events) do
        GenServer.call(producer, {:stage, action, events})
    end

    def stage_events(%Producer{cursor: index, stream: stream}, action, events, stage) 
    when is_list(events) and is_tuple(stream) and is_integer(index) and is_pid(stage) do
        {events, version} = 
            Enum.map_reduce(events, index, fn event, acc -> 
                index = acc + 1
                event = Event.new(event, action, index)
                {event,  index}
            end)
        %Staged{events: events, version: version, stream: stream, stage: stage}
    end

    def process(%Action{stream: stream, app: app}=action) do 
        Events.Supervisor.prepare_producer(app, stream)
        |> GenServer.call({:process, action}, :infinity)
    end

    def stream_id(%Producer{stream: stream}) do
        stream_id(stream)
    end

    def stream_id({_type, id}) do
        "stream:" <> id
    end

    defp calibrate(%Producer{app: app, store: store}=prod) do
        case Kernel.apply(store, :get_index, [app, stream_id(prod)]) do
            nil ->
                %Producer{prod | cursor: 0}

            value when is_integer(value) and value >= 0 ->
                %Producer{prod | cursor: value}
        end
    end

    defp aggregate_state(%Producer{ app: app, stream: stream}, _consistency) do
        Signal.Aggregates.Supervisor.prepare_aggregate(app, stream)
        |> Signal.Aggregates.Aggregate.state()
    end

    defp handle_command(command, params, aggregate) when is_struct(command) do
        case Signal.Command.Handler.handle(command, params, aggregate) do
            {:ok, event} when is_struct(event) ->
                [event]

            {:ok, event} when is_list(event) ->
                event

            event when is_struct(event) ->
                [event]

            event when is_list(event) -> 
                event

            {:error, reason} ->
                Result.error(reason)
        end
    end

    defp confirm_staged(staged) do
        Enum.each(staged, fn %Staged{version: version, stage: stage}->  
            Process.send(stage, {:ok, version}, [])
        end)
    end

    defp rollback_staged(staged, reason) when is_list(staged) do
        opts = [:nosuspend]
        payload = {:rollback, reason}
        Enum.each(staged, fn 
            %Staged{stage: stage} ->  
                Process.send(stage, payload, opts)
            _ ->
                nil
        end)
    end


end


