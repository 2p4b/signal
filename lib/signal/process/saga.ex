defmodule Signal.Process.Saga do
    use GenServer, restart: :transient

    alias Signal.Event
    alias Signal.Codec
    alias Signal.Effect
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

                %Effect{}=effect ->
                    load_saga_state(saga, effect)

                _ -> 
                    state = 
                        saga.module
                        |> Kernel.apply(:init, [saga.id])

                    %Saga{saga| state: state}
            end
            |> sched_next_action()

        Signal.PubSub.subscribe(saga.app, saga.uuid)

        router_push(saga, {saga.status, saga.id, saga.ack})
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

    # no actions in queue to process
    # so continue to stopping saga
    @impl true
    def handle_continue(:process_event, %Saga{status: :stop, actions: []}=saga) do
        {:noreply, saga, {:continue, :stop}}
    end

    # stop processing events
    # so ignore signal
    @impl true
    def handle_continue(:process_event, %Saga{status: :stop}=saga) do
        {:noreply, saga}
    end

    # Do noting because event 
    # buffer is empty
    @impl true
    def handle_continue(:process_event, %Saga{buffer: []}=saga) do
        {:noreply, saga}
    end

    @impl true
    def handle_continue(:process_event, %Saga{buffer: [number| rest]}=saga) 
    when is_integer(number) do

        buffer =
            saga.app
            |> Signal.Store.Adapter.get_event(number)
            |> List.wrap()
            |> Enum.concat(rest)

        handle_continue(:process_event, %Saga{saga| buffer: buffer})
    end

    @impl true
    def handle_continue(:process_event, %Saga{buffer: [%Event{}=event| buffer]}=saga) do
        [
            app: saga.app,
            type: saga.module,
            sid: saga.id,
            uuid: saga.uuid,
            status: saga.status,
            processing: [
                topic: event.topic,
                number: event.number,
            ]
        ]
        |> Signal.Logger.info(label: :saga)

        saga = 
            saga
            |> process_event(event)
            |> struct(%{buffer: buffer})
            |> save_saga_state()

        {:noreply, saga, {:continue, :process_event}}
    end


    def handle_continue(:stop, %Saga{actions: [], buffer: [], status: :stop}=saga) do
        router_push(saga, {:stop, saga.id})
        {:noreply, saga, :hibernate}
    end

    def handle_continue(:stop, %Saga{actions: []}=saga) do
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
    def handle_info(%Event{number: number}=event, %Saga{id: id}=saga) do
        saga = 
            saga
            |> queue_event(event)
            |> save_saga_state()
            |> router_push({:ack, id, number, :running})
        {:noreply, saga, {:continue, :process_event}}
    end

    defp execute(command, %Saga{app: app}, opts) do
        Kernel.apply(app, :dispatch, [command, opts])
    end

    defp queue_event(%Saga{}=saga, %Event{}=event) do
        %Saga{buffer: buffer} = saga
        %Event{number: number} = event

        ack = 
            if event.number > saga.ack do
                event.number
            else
                saga.ack
            end

        found = Enum.find(saga.buffer, fn 
            %Event{number: no} -> no === number 
            no when is_integer(no) -> no === number
        end)

        if found do
            saga
        else
            %Saga{saga| ack: ack, buffer: buffer ++ List.wrap(event)}
        end
    end

    defp save_saga_state(%Saga{app: app}=saga) do
        effect = create_saga_effect(saga)
        :ok = Signal.Store.Adapter.save_effect(app, effect)
        saga
    end

    defp shutdown_saga(%Saga{app: app, uuid: process_uuid}) do
        :ok = Signal.Store.Adapter.delete_effect(app, process_uuid)
    end

    defp process_event(%Saga{}=saga, %Event{}=event) do

        %Saga{state: state, module: module} = saga

        case Kernel.apply(module, :handle_event, [Event.data(event), state]) do 
            {action, params, state} when is_binary(action)  ->
                %Saga{saga| state: state, status: :running}
                |> enqueue_event_action(event, {action, params})

            {:ok, state} ->
                %Saga{saga | state: state, status: :running}


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

                %Saga{saga | state: state, status: :stop}

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

    defp load_saga_state(%Saga{}=saga, %Effect{data: data}) do
        %{
            "ack" => ack, 
            "state" => payload, 
            "buffer" => buffer,
            "status" => status,
            "version" => version, 
        } = data

        status = String.to_existing_atom(status)

        {:ok, state} = 
            saga.module
            |> struct([])
            |> Codec.load(payload)

        %Saga{saga | 
            ack: ack, 
            state: state, 
            buffer: buffer, 
            status: status,
            version: version, 
        }
    end

    def create_saga_effect(%Saga{}=saga) do
        %Saga{
            id: id, 
            ack: ack, 
            state: state, 
            buffer: buffer,
            status: status,
            actions: actions,
            version: version, 
            namespace: namespace,
        } = saga

        event_buffer = 
            Enum.map(buffer, fn 
                %Event{number: number} -> number 
                number when is_integer(number) -> number
            end)

        status = Atom.to_string(status)

        {:ok, payload} = Codec.encode(state)

        data = %{
            "id" => id,
            "ack" => ack, 
            "state" => payload, 
            "status" => status,
            "version" => version, 
            "actions" => actions,
            "buffer" => event_buffer,
        } 

        uuid = Effect.uuid(namespace, id)

        [uuid: uuid, namespace: namespace, data: data]
        |> Effect.new()
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
