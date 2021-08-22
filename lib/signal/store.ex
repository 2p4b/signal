defmodule Signal.Store do

    @type app :: {module::atom, name::atom}

    @type stream :: {module::atom,  id::binary}

    @callback cursor(app::app) :: integer()

    @callback subscribe() :: {:ok, any} | {:error, reason::term()}

    @callback subscribe(name::binary()) :: {:ok, any} | {:error, reason::term()}

    @callback subscribe(name::binary(), opts::list()) :: {:ok, any} | {:error, reason::term()}

    @callback unsubscribe(opts::list()) :: :ok | {:error, reason::term()}

    @callback subscription(opts::list()) :: term() | {:error, reason::term()}

    @callback stream_position(stream::stream)  :: integer()

    @callback stream_position(stream::stream, name :: atom) :: integer()

    @callback record(app::app, log::term()) :: {:ok, integer()} | {:error, reason::term()}

    @callback next(app::app, cursor::integer(), opts::list()) :: term() | nil

    @callback get_index(app::app,  name::String.t) :: integer() | nil

    @callback set_index(app::app, name::String.t, position::integer()) :: {:ok, integer()} | {:error, reason :: term()}

    @callback get_state(app::app, id::String.t) :: {:ok, term()} | {:error, reason :: term()} | nil

    @callback get_event(app::app, number::integer()) :: term() | {:error, reason::term()} | nil

    @callback get_state(app::app, id::String.t, version::integer()) :: {:ok, term()} | {:error, reason::term()} | nil

    @callback set_state(app::app, id::String.t, state::term()) :: {:ok, integer} | {:error, reason::term()}

    @callback set_state(app::app, id::String.t, version::integer(), state::term()) :: {:ok, integer} | {:error, reason::term()}

    @callback list_events(app :: app, topic :: String.t, index :: integer(), count :: integer()) :: list() | {:error, reason :: term()}

end

