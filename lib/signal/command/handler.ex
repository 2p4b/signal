defprotocol Signal.Command.Handler do
    def handle(command, agggregate, params)
end


