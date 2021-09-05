defmodule Signal.Store do

    @type app :: {module::atom, name::atom}

    @type stream :: {module::atom,  id::binary}

    @type handle :: binary() | list()

    @type opts :: list()

    @type enumber :: number()

    @type snapshot :: term()

    @callback index(opts) :: integer()

    @callback event(enumber, opts) :: term() | {:error, reason::term()}

    @callback publish(events::list(), opts) :: :ok | {:error, reason::term()}

    @callback subscribe(handle, opts) :: {:ok, any} | {:error, reason::term()}

    @callback unsubscribe(handle, opts) :: :ok | {:error, reason::term()}

    @callback subscription(handle, opts) :: term() | {:error, reason::term()}

    @callback stream_position(stream::stream, opts)  :: integer()

    @callback record(snapshot, opts) :: {:ok, integer()} | {:error, reason::term()}

    @callback snapshot(id::binary(), opts) :: term() | nil | {:error, reason::term()}

    @callback acknowledge(handle, enumber, opts) :: :ok | {:error, reason::term()}

end

