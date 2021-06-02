defmodule Signal do
    # See https://hexdocs.pm/elixir/Application.html
    # for more information on OTP Applications
    @moduledoc false

    def start(_type, _args) do
        children = [
            #Signal.Application
        ]

        opts = [strategy: :one_for_one, name: Signal.Supervisor]
        Supervisor.start_link(children, opts)
    end

end
