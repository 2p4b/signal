defmodule Signal.Store.Writer do
    alias Signal.PubSub
    alias Signal.Transaction
    alias Signal.Store.Writer
    alias Signal.Stream.Stage
    alias Signal.Store.Adapter

    use GenServer
    use Signal.Telemetry

    require Logger

    defstruct [index: 0, app: nil, streams: %{}]

    @doc """
    Starts a new execution queue.
    """
    def start_link(opts) do
        name = 
            case Keyword.get(opts, :app) do
                nil -> Keyword.get(opts, :name, __MODULE__)
                app -> writer_name(app)
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
        meta = metadata(writer) |> Map.put(:transaction, metadata(transaction))
        # { streams, events }
        start = telemetry_start(:commit, meta, measurements(writer))
        transaction = prepare_transaction(writer, transaction)

        case Adapter.commit_transaction(writer.app, transaction, opts) do
            {:error, error} ->
                meta = Map.put(meta, :error, error)
                telemetry_stop(:commit, start, meta, measurements(writer))
                {:reply, {:error, error}, writer}

            :ok ->
                push_broker_events(writer, transaction)
                writer = %Writer{writer | index: transaction.cursor}
                telemetry_stop(:commit, start, meta, measurements(writer))
                {:reply, :ok, writer}
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

    def writer_name(nil) do
        __MODULE__
    end

    def writer_name(app) when is_atom(app) do
        Module.concat(__MODULE__, app)
    end

    def index(app) do
        writer_name(app)
        |> GenServer.call(:index)
    end

    def commit(app, transaction, opts \\ []) do
        writer_name(app)
        |> GenServer.call({:commit, transaction, opts})
    end

    def measurements(%Writer{}=writer) do
        %{index: writer.index}
    end

    def metadata(%Writer{}=writer) do
        %{app: writer.app, streams: writer.streams}
    end

    def metadata(%Transaction{}=trnx) do
        %{uuid: trnx.uuid, cursor: trnx.cursor}
    end

end

