defmodule PGWire.Application do
  use Application

  def start(_type, _args) do
    children = [
      :ranch.child_spec(
        :pg_wire,
        :ranch_tcp,
        [{:port, 5432}],
        PGWire.Connection,
        []
      )
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
