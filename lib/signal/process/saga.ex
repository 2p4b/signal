defmodule Signal.Process.Saga do
    use GenServer, restart: :transient

    alias Signal.Event
    alias Signal.Codec
    alias Signal.Result
    alias Signal.Process.Saga
    alias Signal.Process.Supervisor

    defstruct [
        :id, 
        :app,
        :uuid,
        :state, 
        :start,
        :module, 
        :namespace,
        :channel,
        ack: 0, 
        buffer: [],
        version: 0,
        actions: [],
        timeout: 5000,
        status: :start,
    ]

    @doc """
    Starts a new process.
    """
    def start_link(opts) do
        name = Keyword.get(opts, :name)
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        {:ok, struct(__MODULE__, opts), {:continue, :load_effect}}
    end

    def start(app, {id, module}, opts \\ []) do
        Supervisor.prepare_saga(app, {id, module}, opts)    
    end

    @impl true
    def handle_continue(:load_effect, %Saga{start: start}=saga) do
        saga =
            case Signal.Store.Adapter.get_effect(saga.app, saga.uuid) do
                # incomplete previous process state shutdown
                # cleanup clean up and start new state
                # there is a better way to handle this
                # just nothing comes to mind at the
                # moment to handle this case more...
                # gracefully
                %{data: %{"ack" => ack, "status" => "stop"}} when start > ack ->
                    # shutdown the saga
                    shutdown_saga(saga)
                    saga.module
                    |> Kernel.apply(:init, [saga.id])

                %Signal.Effect{}=effect ->
                    load_saga_state(saga, effect)

                _ -> 
                    state = 
                        saga.module
                        |> Kernel.apply(:init, [saga.id])

                    %Saga{saga| state: state}
            end

        Signal.PubSub.subscribe(saga.app, saga.uuid)

        router_push(saga, {saga.status, saga.id, saga.start})
        [
            app: saga.app,
            sid: saga.id,
            spid: saga.uuid,
            namespace: saga.namespace,
            status: saga.status,
            loaded: saga.version,
            timeout: saga.timeout,
        ]
        |> Signal.Logger.info(label: :saga)
        {:noreply, saga, saga.timeout}
    end

    def handle_continue(:stop, %Saga{actions: [], status: :stop}=saga) do
        {:noreply, notify_router_ack(saga)}
    end

    def handle_continue(:stop, %Saga{}=saga) do
        {:noreply, saga}
    end

    @impl true
    def handle_continue({:dispatch, command, action}, %Saga{}=saga) do
        action_name = Map.get(action, "name")
        action_uuid = Map.get(action, "uuid")
        action_cause = Map.get(action, "cause")
        causation_id = Map.get(action, "causation_id")

        opts = [
            causation_id: causation_id,
            correlation_id: action_uuid
        ]

        [
            app: saga.app,
            type: saga.module,
            sid: saga.id,
            cause: action_cause,
            action: action_name,
            dispatch: command.__struct__,
        ]
        |> Signal.Logger.info(label: :saga)

        case execute(command, saga, opts) do
            {:ok, %Result{}} ->
                saga = 
                    saga
                    |> drop_action(action_uuid)
                    |> sched_next_action()
                {:noreply, saga, saga.timeout}

            {:error, error} ->
                continue = {:action_error, action, command, error}
                {:noreply, saga, {:continue, continue}}
        end
    end

    @impl true
    def handle_continue({:action_error, action, command, error}, %Saga{}=saga) do
          action_name = Map.get(action, "name")
          action_uuid = Map.get(action, "uuid")
          action_params = Map.get(action, "params")

          args = [{action_name, action_params, command, error}, saga.state]

          case Kernel.apply(saga.module, :handle_error, args)  do
              {:ok, state} ->
                  saga = 
                      %Saga{saga | state: state}
                      |> drop_action(action_uuid)
                      |> save_saga_state()
                      |> sched_next_action()
                  {:noreply, saga, saga.timeout}

              {action_name, params, state} when is_binary(action)  ->
                  saga = 
                      %Saga{saga | state: state}
                      |> enqueue_action_action(action_uuid, {action_name, params})
                      |> save_saga_state()
                  {:noreply, saga, saga.timeout}

              {:retry, name, params, state} ->
                  saga = 
                      %Saga{saga | state: state}
                      |> requeue_action_inplace(action_uuid, {name, params})
                      |> save_saga_state()
                      |> sched_next_action()
                  {:noreply, saga}

                invalid_value ->
                    raise """
                        Invalid saga return value
                        namespace: #{saga.namespace}
                        id: #{saga.id}
                        expected: {:ok, state} | {:retry, {name, params}, state}
                        got: #{invalid_value}
                    """
          end
    end

    @impl true
    def handle_info(:stopped, %Saga{actions: [], status: :stop}=saga) do
        shutdown_saga(saga)
        {:stop, :normal, saga}
    end

    @impl true
    def handle_info({:action, :stop}, %Saga{actions: [], status: :stop}=saga) do
        {:noreply, saga, {:continue, :stop}}
    end

    @impl true
    def handle_info({:action, _id}, %Saga{actions: []}=saga) do
        {:noreply, saga}
    end

    @impl true
    def handle_info({:action, id}, %Saga{actions: [%{"uuid" => uuid}|_]}=saga) 
    when id !== uuid do
        {:noreply, saga}
    end

    @impl true
    def handle_info({:action, _action_uuid}, %Saga{}=saga) do
        [action| actions] = saga.actions

        [app: saga.app, action: action]
        |> Signal.Logger.info(label: :saga)

        action_name = Map.get(action, "name")
        action_params = Map.get(action, "params")

        action_tuple = {action_name, action_params}
        args = [action_tuple, saga.state]

        case Kernel.apply(saga.module, :handle_action, args) do
            {:dispatch, command}->
                {:noreply, saga, {:continue, {:dispatch, command, action}}}

            {:ok, state}->
                saga =
                    %Saga{saga| state: state, actions: actions}
                    |> save_saga_state()
                    |> sched_next_action()
                {:noreply, saga, saga.timeout}

            invalid_value ->
                raise """
                    Invalid saga return value
                    namespace: #{saga.namespace}
                    id: #{saga.id}
                    expected: {:dispatch, command} | {:ok, new_state}
                    got: #{invalid_value}
                """
        end
    end

    @impl true
    def handle_info(:timeout, %Saga{}=saga) do
        {:noreply, saga, :hibernate}
    end

    @impl true
    def handle_info(%Event{number: number}, %Saga{ack: ack}=saga)
    when number <= ack do
        {:noreply, saga}
    end

    @impl true
    def handle_info(%Event{}=event, %Saga{}=saga) do

        %Saga{module: module, state: state} = saga

        [
            app: saga.app,
            type: module,
            sid: saga.id,
            status: :running,
            apply: event.topic,
            number: event.number,
        ]
        |> Signal.Logger.info(label: :saga)

        reply = Kernel.apply(module, :handle_event, [Event.data(event), state])

        handle_reply(saga, event, reply)
    end

    defp execute(command, %Saga{app: app}, opts) do
        Kernel.apply(app, :dispatch, [command, opts])
    end

    defp acknowledge_event(%Saga{ack: ack}=saga, number, _) when ack > number do
        saga
    end

    defp acknowledge_event(%Saga{ack: ack}=saga, number, status) when ack == number do
        %Saga{saga | status: status}
    end

    defp acknowledge_event(%Saga{version: version}=saga, number, status) do
        %Saga{saga | ack: number, status: status, version: version + 1}
    end

    defp commit_state_and_notify_router(%Saga{}=saga) do
        saga
        |> save_saga_state()
        |> notify_router_ack()
    end

    defp notify_router_ack(%Saga{id: id, ack: ack, status: status}=saga) do
        router_push(saga, {:ack, id, ack, status})
    end

    defp save_saga_state(%Saga{app: app}=saga) do
        effect = create_saga_effect(saga)
        :ok = Signal.Store.Adapter.save_effect(app, effect)
        saga
    end

    defp shutdown_saga(%Saga{app: app, uuid: process_uuid}) do
        :ok = Signal.Store.Adapter.delete_effect(app, process_uuid)
    end

    defp handle_reply(%Saga{}=saga, %Event{number: number}=event, reply) do
        case reply do 
            {action, params, state} when is_binary(action)  ->
                saga = 
                    %Saga{saga| state: state}
                    |> enqueue_event_action(event, {action, params})
                    |> acknowledge_event(number, :running)
                    |> commit_state_and_notify_router()
                {:noreply, saga}

            {:ok, state} ->
                saga = 
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :running)
                    |> commit_state_and_notify_router()
                {:noreply, saga, saga.timeout}

            {:sleep, state} ->
                saga = 
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :running)
                    |> commit_state_and_notify_router()
                {:noreply, saga, :hibernate}

            {:hibernate, state} ->
                saga = 
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :sleep)
                    |> commit_state_and_notify_router()
                {:noreply, saga}


            {:stop, state} ->
                [
                    app: saga.app,
                    type: saga.module,
                    sid: saga.id,
                    event: event.topic,
                    status: :stopped,
                    number: event.number,
                ]
                |> Signal.Logger.info(label: :saga)

                saga =
                    %Saga{saga | state: state}
                    |> acknowledge_event(number, :stop)
                    |> save_saga_state()

                {:noreply, saga, {:continue, :stop}}

            invalid_value ->
                raise """
                    Invalid saga return value
                    namespace: #{saga.namespace}
                    id: #{saga.id}
                    event: #{inspect(Event.data(event))}
                    reply: #{inspect(invalid_value)}
                """
        end
    end

    defp load_saga_state(%Saga{}=saga, %Signal.Effect{data: data}) do
        %{
            "ack" => ack, 
            "state" => payload, 
            "status" => status,
            "version" => version, 
        } = data

        status = String.to_existing_atom(status)

        {:ok, state} = 
            saga.module
            |> struct([])
            |> Codec.load(payload)
        %Saga{saga | state: state, ack: ack, version: version, status: status}
    end

    def create_saga_effect(%Saga{}=saga) do
        %Saga{
            id: id, 
            ack: ack, 
            state: state, 
            status: status,
            actions: actions,
            version: version, 
            namespace: namespace,
        } = saga

        status = Atom.to_string(status)

        {:ok, payload} = Codec.encode(state)

        data = %{
            "id" => id,
            "ack" => ack, 
            "state" => payload, 
            "status" => status,
            "version" => version, 
            "actions" => actions,
        } 

        uuid = Signal.Effect.uuid(namespace, id)

        [uuid: uuid, namespace: namespace, data: data]
        |> Signal.Effect.new()
    end

    defp enqueue_action_action(%Saga{}=saga, action_uuid, {name, params}) do
        action = Enum.find(saga.actions, &(Map.get(&1, "uuid") == action_uuid))
        params = encode_action_params(params)

        updated_action = 
            action
            |> Map.put("name", name)
            |> Map.put("params", params)

        actions = 
            saga
            |> drop_action(action_uuid)
            |> Map.get(:actions)
            |> Enum.concat(List.wrap(updated_action))

        send(self(), {:action, action_uuid})
        %Saga{saga | actions: actions}
    end

    defp enqueue_event_action(%Saga{}=saga, event, {name, params}) do
        action = make_event_action(saga, event, name, params)
        send(self(), {:action, Map.get(action, "uuid")})
        %Saga{saga | actions: saga.actions ++ List.wrap(action)}
    end

    defp requeue_action_inplace(%Saga{}=saga, id, {name, params}) do
        params = encode_action_params(params)
        index = 
            saga.actions
            |> Enum.find_index(&(Map.get(&1, "uuid") == id))

        action = 
            saga.actions
            |> Enum.at(index)
            |> Map.put("name", name)
            |> Map.put("params", params)
            |> Map.update!("tries", &(&1+1))

        actions = List.replace_at(saga.actions, index, action)

        send(self(), {:action, Map.get(action, "uuid")})
        %Saga{saga | actions: actions}
    end

    defp sched_next_action(%Saga{actions: [], status: :stop}=saga) do
        send(self(), {:action, :stop})
        saga
    end

    defp sched_next_action(%Saga{actions: []}=saga) do
        saga
    end

    defp sched_next_action(%Saga{actions: [action|_]}=saga) do
        send(self(), {:action, Map.get(action, "uuid")})
        saga
    end

    defp drop_action(%Saga{}=saga, id) do
        actions = 
            saga.actions
            |> Enum.filter(&(Map.get(&1, "uuid") !== id))
        %Saga{saga | actions: actions}
    end

    defp make_event_action(%Saga{}=saga, %Event{}=event, name, params) do
        uuid = UUID.uuid5(saga.uuid, event.uuid)
        %{
            "name" => name,
            "uuid" => uuid,
            "tries" => 0,
            "cause" => event.topic,
            "params" => encode_action_params(params),
            "timestamp" => DateTime.utc_now(),
            "causation_id" => event.uuid,
        }
    end

    defp encode_action_params(params) when is_map(params) do
        {:ok, data} = Codec.encode(params)
        data
    end

    defp encode_action_params(%DateTime{}=v) do
        String.Chars.to_string(v)
    end

    defp encode_action_params(v) 
    when is_binary(v) or is_integer(v) or is_float(v)  do
        v
    end

    defp encode_action_params(nil) do
        nil
    end

    defp encode_action_params(v) when is_atom(v) do
        Atom.to_string(v)
    end

    defp encode_action_params(v) do
        raise ArgumentError, message: "unable to encode action params:\n #{v}"
    end
    
    def router_push(%Saga{}=saga, payload) do
        Signal.PubSub.broadcast(saga.app, saga.channel, payload)
        saga
    end

    @impl true
    def terminate(_, _) do
        :ok
    end

end
