defmodule Signal.Channels.Channel do

    use GenServer
    alias Signal.Helper
    alias Signal.Events.Event
    alias Signal.Subscription
    alias Signal.Channels.Channel
    require Logger

    defstruct [:name, :index, :app, :syn, :ack, :subscriptions, :topics, :store]

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
        app = Keyword.get(opts, :app)
        name = Keyword.get(opts, :id)
        store = Keyword.get(opts, :store)
        args = [
            ack: 0,
            syn: 0,
            app: app,
            name: name,
            store: store,
            index: 0,
            topics: [],
            subscriptions: [],
        ]
        {:ok, struct(__MODULE__, args)}
    end

    @impl true
    def handle_info(:init, %Channel{name: name, app: app, store: store}=state) do
        ack = 
            case store.get_index(app, name) do
                nil -> 0
                value -> value
            end
        Signal.Application.listen(app)
        index = Signal.Events.Recorder.cursor(app)
        {:noreply, %Channel{state | index: index, syn: ack, ack: ack} }
    end

    @impl true
    def handle_info(:pull, state) do
        state =
            case pull_event(state) do
                nil -> 
                    state
                {:error, _reason} ->
                    sched_next()

                %Event{topic: topic, number: number}=event ->
                    if topic in state.topics do
                        push_event(event, state)
                    else
                        handle_ack(number, state)
                    end
            end
        {:noreply, state} 
    end

    @impl true
    def handle_info(%Event{topic: topic, number: number}=event, %Channel{topics: topics}=state) do
        state = 
            if topic in topics do
                handle_event(event, state)
            else
                if (state.ack + 1) == number do
                    handle_ack(number, state)
                else
                    state
                end
            end
        {:noreply, %Channel{state | index: number}}
    end

    @impl true
    def handle_info({:ack, pid, number}, %Channel{subscriptions: subscriptions}=state) do
        index = 
            subscriptions
            |> Enum.find_index(fn %Subscription{consumer: {_ref, spid}} -> 
                spid == pid
            end)

        subscription = Enum.at(subscriptions, index) |> Map.put(:ack, number)
        subscriptions = List.replace_at(subscriptions, index, subscription)
        {:noreply, handle_ack(number, %Channel{ state| subscriptions: subscriptions })} 
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _obj, _reason}, %Channel{}=state) do
        %Channel{subscriptions: subscriptions} = state

        sindex = 
            subscriptions
            |> Enum.find_index(fn %Subscription{consumer: {sref, _pid}} -> 
                sref == ref 
            end)

        subscriptions = List.delete_at(subscriptions, sindex)

        channel =
            if Enum.empty?(subscriptions) do
                %Channel{app: app, name: name} = state
                Signal.Channels.Supervisor.stop_child(app, name)
                state
            else
                successor = Enum.max(subscriptions, &Map.get(&1, :ack))
                if successor.syn == successor.ack do
                    sched_next()
                end
                update_topics(%Channel{state | subscriptions: subscriptions, ack: successor.ack})
            end
        {:noreply, channel}
    end

    @impl true
    def handle_call({:syn, position}, _from, %Channel{}=state) do
        state = %Channel{state | syn: position}
        {:reply, state, state}
    end

    @impl true
    def handle_call(:state, _from, state) do
        {:reply, state, state}
    end

    @impl true
    def handle_call({:subscribe, topic, opts}, from, state) when is_binary(topic) do
        handle_call({:subscribe, [topic], opts}, from, state)
    end

    @impl true
    def handle_call({:subscribe, topics, opts}, {pid, _ref}, %Channel{}=state) 
    when is_list(topics) do
        {sub, channel} = handle_subscribe(state, topics, pid, opts)

        # Do not pull for event if channel
        # is waiting for an ack request
        if state.syn == state.ack and state.index > state.ack do
            sched_next()
        end

        channel = update_topics(channel)

        info = """
        [SUBSCRIBED] #{state.name}
            cid: #{inspect(self())}
            pid: #{inspect(pid)}  
            syn: #{inspect(sub.syn)}
            ack: #{inspect(sub.ack)}
            topics: #{inspect(sub.topics)}
            listening: #{inspect(channel.topics)}
        """
        Logger.info(info)

        {:reply, sub, channel} 
    end

    @impl true
    def handle_call({:unsubscribe, topics}, {pid, _ref}, %Channel{}=state) 
    when is_list(topics) do
        {sub, channel} = handle_unsubscribe(state, topics, pid)
        {:reply, sub, update_topics(channel)} 
    end

    defp sched_next() do
        Process.send(self(), :pull, [])
    end

    defp update_topics(%Channel{subscriptions: subs}=state) do
        topics =
            subs
            |> Enum.reduce([], fn %Subscription{topics: topics}, acc -> 
                acc ++ topics 
            end)
            |> Enum.uniq()

        %Channel{state | topics: topics}
    end

    defp handle_subscribe(%Channel{subscriptions: subs}=state, topics, pid, opts) 
    when is_pid(pid) and is_list(topics) do

        subscription = 
            subs
            |> Enum.find(&(Map.get(&1, :consumer) |> elem(1) == pid))

        {sub, subs} = 
            if is_nil(subscription) do
                ack = Keyword.get(opts, :start, state.syn)
                sub = %Subscription{ 
                    ack: ack,
                    syn: ack,
                    topics: Enum.uniq(topics), 
                    channel: state.name,
                    consumer: {Process.monitor(pid), pid},
                }
                {sub, subs ++ [sub]}
            else
                unique_pid = &(Map.get(&1, :consumer) |> elem(1))
                sub =
                    %Subscription{subscription | 
                        topics: Enum.uniq(subscription.topics ++ topics)
                    }
                {sub, Enum.uniq_by([sub] ++ subs, unique_pid)}
            end

        syn  =
            if length(subs) == 1 do
                sub.ack
            else
                state.syn
            end

        {sub, %Channel{ state | subscriptions: subs, syn: syn}}
    end

    defp handle_unsubscribe(%Channel{subscriptions: subs}=state, topics, pid) 
    when is_pid(pid) and is_list(topics) do

        subscription = 
            subs
            |> Enum.find(&(Map.get(&1, :consumer) |> elem(1) == pid))

        {sub, subs} = 
            if is_nil(subscription) do
                {nil, subs}
            else
                filter = &(not(Enum.member?(topics, &1)))
                topics = Enum.filter(subscription.topics, filter)

                if length(topics) == 0 do 
                    {ref, pid} = subscription.consumer
                    Process.demonitor(ref)
                    filter = &(Map.get(&1, :consumer) |> elem(1) != pid)
                    sub = %Subscription{subscription | 
                        topics: topics, 
                        consumer: {nil, pid} 
                    }
                    {sub, Enum.filter(subs, filter)}
                else 
                    unique_pid = &(Map.get(&1, :consumer) |> elem(1))
                    sub = %Subscription{subscription | topics: topics}
                    {sub, Enum.uniq_by([sub] ++ subs, unique_pid)}
                end

            end

        {sub, %Channel{ state | subscriptions: subs}}
    end

    defp handle_event(%Event{}=event, %Channel{ack: ack, index: index}=channel) do
        if ack == index do
            push_event(event, channel)
        else
            channel
        end
    end

    defp pull_event(%Channel{}=channel) do
        %Channel{app: app, ack: ack, store: store} = channel
        store.get_event(app, ack + 1)
    end

    defp push_event(%Event{topic: topic, number: number}=event, %Channel{}=channel) do
        index =
            channel.subscriptions
            |> Enum.find_index(fn %Subscription{topics: topics} -> 
                Enum.member?(topics, topic)
            end)

        sub = Enum.at(channel.subscriptions, index)

        with %Subscription{consumer: {_ref, pid}} <- sub do
            Process.send(pid, event, [])
        end

        subscriptions = 
            channel.subscriptions
            |> List.replace_at(index, %Subscription{sub | syn: number})

        %Channel{channel | syn: number, subscriptions: subscriptions}
    end

    defp handle_ack(number, %Channel{index: index, ack: ack, store: store}=channel) 
    when number > ack do
        {:ok, ack} = store.set_index(channel.app, channel.name, number)
        if number < index do
            sched_next()
        end
        %Channel{channel| ack: ack}
    end

    defp handle_ack(_number, %Channel{}=channel) do
        channel
    end

    def subscribe(app, handle, topics, opts \\ [])

    def subscribe(app, handle, topics, opts) when is_atom(handle) do
        subscribe(app, Helper.module_to_string(handle), topics, opts)
    end

    def subscribe(app, handle, topics, opts) when is_binary(handle) do
        opts = 
            case Keyword.fetch(opts, :start) do

                {:ok, value} when is_integer(value) -> opts

                {:ok, :now} -> 
                    Keyword.replace(opts, :start, app.index())

                {:ok, :beginning} ->
                    Keyword.replace(opts, :start, 0)

                _smt -> opts
            end
        app
        |> Signal.Channels.Supervisor.prepare_channel(handle)
        |> GenServer.call({:subscribe, topics, opts}, 5000)
    end

    def unsubscribe(app, handle, topics) when is_binary(handle) do
        app
        |> Signal.Channels.Supervisor.prepare_channel(handle)
        |> GenServer.call({:unsubscribe, topics}, 5000)
    end

    def acknowledge(app, handle, number) 
    when is_binary(handle) and is_integer(number) do
        app
        |> Signal.Channels.Supervisor.prepare_channel(handle)
        |> GenServer.whereis()
        |> Process.send({:ack, self(), number}, [])
    end

    def syncronize(app, handle, position) do
        app
        |> Signal.Channels.Supervisor.prepare_channel(handle)
        |> GenServer.call({:syn, position}, 5000)
    end

end
