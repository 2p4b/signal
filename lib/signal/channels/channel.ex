defmodule Signal.Channels.Channel do

    use GenServer
    alias Signal.Helper
    alias Signal.Events.Event
    alias Signal.Subscription
    alias Signal.Channels.Channel

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
            index: 0,
            app: app,
            name: name,
            store: store,
            topics: [],
            subscriptions: [],
        ]
        {:ok, struct(__MODULE__, args)}
    end

    @impl true
    def handle_info(:init, %Channel{name: name, app: app, store: store}=state) do
        index = 
            case store.get_index(app, name) do
                nil -> 0
                value -> value
            end
        Signal.Application.listen(app)
        {:noreply, %Channel{state | syn: index, ack: index} }
    end

    @impl true
    def handle_info(:pull, state) do
        state =
            case pull_events(state) do
                [event] ->
                    push_event(event, state)
                [] ->
                    state
            end
        {:noreply, state} 
    end

    @impl true
    def handle_info(%Event{topic: topic, number: number}=event, %Channel{topics: topics}=state) do
        state = 
            if topic in topics do
                handle_event(event, state)
            else
                state
            end
        {:noreply, %Channel{state | index: number}}
    end

    @impl true
    def handle_info({:ack, number}, %Channel{}=state) do
        {:noreply, handle_ack(number, state)} 
    end

    @impl true
    def handle_info({:DOWN, ref, :process, _obj, _reason}, %Channel{}=state) do
        subscriptions = 
            state.subscriptions
            |> Enum.filter(fn %Subscription{consumer: {sref, _pid}} -> 
                sref != ref 
            end)

        {:noreply, %Channel{state | subscriptions: subscriptions}}
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
        if state.syn == state.ack do
            sched_next()
        end
        {:reply, sub, update_topics(channel)} 
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
                sub = %Subscription{ 
                    ack: Keyword.get(opts, :start, state.syn),
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
                    sub = %Subscription{subscription | topics: topics, consumer: {nil, pid} }
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

    defp pull_events(%Channel{topics: topics, app: app, syn: syn, store: store}) do
        store.list_events(app, topics, syn, 1)
    end

    defp push_event(%Event{topic: topic, number: number}=event, %Channel{}=channel) do
        sub =
            channel.subscriptions
            |> Enum.find(fn %Subscription{topics: topics} -> 
                Enum.member?(topics, topic)
            end)

        with %Subscription{consumer: {_ref, pid}} <- sub do
            Process.send(pid, event, [])
        end
        %Channel{channel | syn: number}
    end

    defp handle_ack(number, %Channel{syn: syn, app: app, store: store, name: name}=channel) 
    when number == syn do
        sched_next()
        {:ok, ack} = store.set_index(app, name, number)
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
        |> Process.send({:ack, number}, [])
    end

    def syncronize(app, handle, position) do
        app
        |> Signal.Channels.Supervisor.prepare_channel(handle)
        |> GenServer.call({:syn, position}, 5000)
    end

end
