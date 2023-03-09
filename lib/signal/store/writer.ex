defmodule Signal.Store.Writer do
    alias Signal.PubSub
    alias Signal.Transaction
    alias Signal.Store.Writer
    alias Signal.Stream.Stage
    alias Signal.Store.Adapter

    use GenServer

    require Logger

    defstruct [index: 0, app: nil, streams: %{}]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = 
            case Keyword.get(opts, :app) do
                nil ->
                    Keyword.get(opts, :name, __MODULE__)

                app ->
                    name = Keyword.get(opts, :name, __MODULE__)
                    Module.concat(app, name)
            end
        GenServer.start_link(__MODULE__, opts, name: name)
    end

    @impl true
    def init(opts) do
        app = Keyword.get(opts, :app) 
        index = Adapter.get_cursor(app)
        {:ok, struct(__MODULE__, Keyword.merge(opts, [index: index]))}
    end

    @impl true
    def handle_call(:index, _from, writer) do
        {:reply, writer.index, writer}
    end

    @impl true
    def handle_call({:commit, %Transaction{}=transaction, opts}, _from, writer) do
        # { streams, events }
        transaction = prepare_transaction(writer, transaction)

        case Adapter.commit_transaction(writer.app, transaction, opts) do
            {:error, error} ->
                {:reply, {:error, error}, writer}

            :ok ->
                push_broker_events(writer, transaction)
                {:reply, :ok, %Writer{writer | index: transaction.cursor}}
        end
    end

    def push_broker_events(%Writer{}=writer, %Transaction{}=trnx) do
        %Writer{app: app} = writer
        %Transaction{staged: staged} = trnx
        staged
        |> Enum.each(fn %Stage{events: events} -> 
            Enum.each(events, fn event -> 
                PubSub.broadcast_event(app, event)
            end)
        end)
    end

    def prepare_transaction(%Writer{index: index}, %Transaction{}=trxn) do
        {staged, index} = 
            trxn.staged
            |> Enum.map_reduce(index, fn stage, index -> 
                {events, index} = 
                    stage.events
                    |> Enum.map_reduce(index, fn event, index -> 
                        number = index + 1
                        params =
                            event
                            |> Map.from_struct()
                            |> Map.put(:number, number)
                        {struct(Signal.Event, params), number}
                    end)
                {%Stage{stage | events: events}, index}
            end)
        %Transaction{trxn | staged: staged, cursor: index}
    end

    def name(application) when is_atom(application) do
        Module.concat(application, __MODULE__)
    end

    def index(application) do
        application
        |> name()
        |> GenServer.call(:index)
    end

    def commit(application, transaction, opts \\ []) do
        application
        |> name()
        |> GenServer.call({:commit, transaction, opts})
        #|> Enum.map(fn event -> 
            ##appilcation.broadcast(event)
            #info = """

            #[PUBLISHER] 
            #published #{event.topic}
            #stream: #{event.stream_id}
            #number: #{event.number}
            #position: #{event.position}
        #"""
            #Logger.info(info)
        #end)
        #:ok
    end

end

