defmodule Signal.WriterFactory do
    use ExMachina

    def transaction_factory do
        Signal.Transaction.new([])
    end

    def writer_factory do
        %Signal.Store.Writer{
            index: 0, 
            streams: %{}, 
        }
    end

end
