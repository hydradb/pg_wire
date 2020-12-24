defmodule PGWire.MixProject do
  use Mix.Project

  @version "0.0.2"

  def project do
    [
      app: :pg_wire,
      version: @version,
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      description: "The PostgreSQL backend protocol for Elixir.",
      deps: deps(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    %{
      maintainers: ["Simon Thörnqvist"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/hydradb/pg_wire"
      }
    }
  end

  defp deps do
    [
      {:ranch, "~> 2.0"},
      {:jason, "~> 1.2"},
      {:postgrex, ">= 0.0.0", only: [:dev, :test]},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end
end
