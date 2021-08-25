defmodule Signal.Channels.Channel do

    use GenServer
    alias Signal.Helper
    alias Signal.Stream.Event
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
    def handle_info(%Event{topic: topic, number: number}=event, %Channel{topics: topics}=state) do
        state = 
            if topic in topics do
                push_event(state, event)
            else
                state
            end
        {:noreply, %Channel{state | syn: number}}
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
        {:noreply, handle_ack(%Channel{state | subscriptions: subscriptions}, number)} 
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

        channel = update_topics(channel)

        #info = """
        #[SUBSCRIBED] #{state.name}
        #    cid: #{inspect(self())}
        #    pid: #{inspect(pid)}  
        #    syn: #{inspect(sub.syn)}
        #    ack: #{inspect(sub.ack)}
        #    topics: #{inspect(sub.topics)}
        #    listening: #{inspect(channel.topics)}
        #"""
        #Logger.info(info)

        %{name: name, app: {app_module, _}} = channel
        {:ok, %{ack: ack, syn: syn}} = app_module.subscribe(name, [topics: topics])
        {:reply, sub, %Channel{channel | syn: syn, ack: ack}} 
    end

    @impl true
    def handle_call({:unsubscribe, topics}, {pid, _ref}, %Channel{}=state) 
    when is_list(topics) do
        {sub, channel} = handle_unsubscribe(state, topics, pid)
        {:reply, sub, update_topics(channel)} 
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

    defp push_event(%Channel{}=channel, %Event{topic: topic, number: number}=event) do
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

    defp handle_ack(%Channel{ack: ack, app: app}=channel, number) when number > ack do
        {app_module, _tenant} = app
        app_module.acknowledge(number)
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
