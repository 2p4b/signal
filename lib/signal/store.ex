defmodule Signal.Store do

    @type opts :: list()
    
    @type snapshot :: term()

    @type effect :: term()

    @type event :: term()

    @type iden :: {id::binary, type::binary} | binary()

    @callback get_effect(uuid::binary, opts) :: effect

    @callback save_effect(effect::effect, opts) :: :ok | {:error, term()}

    @callback delete_effect(uuid::binary, opts) :: :ok | {:error, term()}

    @callback get_cursor(opts::list) :: integer

    @callback commit_transaction(transaction::term(), opts) :: :ok

    @callback handler_position(handle::binary, opts::list) :: integer()

    @callback handler_acknowledge(handle::binary, number::integer, opts::list) :: :ok

    @callback record_snapshot(snapshot::snapshot, opts::list) :: :ok

    @callback delete_snapshot(id::binary, opts::list) :: :ok

    @callback get_snapshot(id::binary, opts::list) :: snapshot()

    @callback read_events(reader::term, opts::list) :: :ok

    @callback list_events(params::list, opts::list) :: list()

    @callback read_stream_events(sid::binary, reader::term, opts::list) :: :ok

    @callback list_stream_events(sid::binary, params::list, opts::list) :: list()

    @callback stream_position(id::binary, opts::list) :: integer()

end

