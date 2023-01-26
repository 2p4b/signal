defmodule Signal.Store do

    @type opts :: list()
    
    @type snapshot :: term()

    @type event :: term()

    @type iden :: {id::binary, type::binary} | binary()

    #@callback get_event(number::integer, opts::list) :: event

    @callback get_cursor(opts::list) :: integer

    @callback commit_transaction(transaction::term(), opts) :: :ok

    @callback handler_position(handle::binary, opts::list) :: integer()

    @callback handler_acknowledge(handle::binary, number::integer, opts::list) :: :ok

    @callback record_snapshot(snapshot::snapshot, opts::list) :: :ok

    @callback delete_snapshot(iden::binary, opts::list) :: :ok

    @callback get_snapshot(iden::binary, opts::list) :: snapshot()

    @callback read_events(reader::term, opts::list) :: :ok

    @callback list_events(opts::list) :: list()

    @callback stream_position(id::binary, opts::list) :: integer()

end

