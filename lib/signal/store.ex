defmodule Signal.Store do

    @type app :: {module::atom, name::atom}

    @type stream :: {module::atom,  id::binary}

    @type iden :: {module::atom, id::binary} | binary

    @type sub_handle :: binary() | list()

    @callback cursor(app::app) :: integer()

    @callback publish(events::list()) :: :ok | {:error, reason::term()}

    @callback subscribe(handle::sub_handle) :: {:ok, any} | {:error, reason::term()}

    @callback subscribe(name::binary(), opts::list()) :: {:ok, any} | {:error, reason::term()}

    @callback unsubscribe(opts::list()) :: :ok | {:error, reason::term()}

    @callback subscription(opts::list()) :: term() | {:error, reason::term()}

    @callback stream_position(stream::stream)  :: integer()

    @callback stream_position(stream::stream, name :: atom) :: integer()

    @callback record(snapshot::term(), opts::list()) :: {:ok, integer()} | {:error, reason::term()}

    @callback snapshot(iden::iden, opts::list()) :: term() | nil | {:error, reason::term()}

    @callback acknowledge(number::integer, opts::list()) :: :ok | {:error, reason::term()}

end

