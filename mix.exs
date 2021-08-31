defmodule Signal.MixProject do
    use Mix.Project

    def project do
        [
            app: :signal,
            version: "0.1.0",
            build_path: "../../_build",
            config_path: "../../config/config.exs",
            deps_path: "../../deps",
            lockfile: "../../mix.lock",
            elixir: "~> 1.11",
            elixirc_paths: elixirc_paths(Mix.env()),
            start_permanent: Mix.env() == :prod,
            consolidate_protocols: Mix.env() != :test,
            deps: deps()
        ]
    end

    # Run "mix help compile.app" to learn about applications.
    def application do
        [
            extra_applications: [:logger],
            mod: {Signal, []}
        ]
    end

    # Specifies which paths to compile per environment.
    defp elixirc_paths(:test), do: ["lib", "test/support"]
    defp elixirc_paths(_), do: ["lib"]

    # Run "mix help deps" to learn about dependencies.
    defp deps do
        [
            {:uuid, "~> 1.1" },
            {:jason, "~> 1.0"},
            {:timex, "~> 3.6.1"},
            {:phoenix_pubsub, "~> 2.0"},
            # {:dep_from_hexpm, "~> 0.3.0"},
            # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
            # {:sibling_app_in_umbrella, in_umbrella: true}
        ]
    end
end
