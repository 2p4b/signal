defmodule Signal.Store do

    @type app :: {module::atom, name::atom}

    @type stream :: {module::atom,  id::binary}

    @type handle :: binary() | list()

    @callback index(opts::list()) :: integer()

    @callback event(number::integer(), opts::list()) :: term() | {:error, reason::term()}
    @callback publish(events::list()) :: :ok | {:error, reason::term()}

    @callback subscribe(handle) :: {:ok, any} | {:error, reason::term()}

    @callback subscribe(handle, opts::list()) :: {:ok, any} | {:error, reason::term()}

    @callback unsubscribe(opts::list()) :: :ok | {:error, reason::term()}

    @callback subscription(opts::list()) :: term() | {:error, reason::term()}

    @callback stream_position(stream::stream)  :: integer()

    @callback stream_position(stream::stream, name :: atom) :: integer()

    @callback record(snapshot::term(), opts::list()) :: {:ok, integer()} | {:error, reason::term()}

    @callback snapshot(id::binary(), opts::list()) :: term() | nil | {:error, reason::term()}

    @callback acknowledge(handle, number::integer, opts::list()) :: :ok | {:error, reason::term()}

end

