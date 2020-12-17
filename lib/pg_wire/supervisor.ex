defmodule PGWire.Supervisor do
  use Supervisor

  @default_port 5432

  def start_link(opts) do
    port = Keyword.get(opts, :port, @default_port)
    protocol = Keyword.fetch!(opts, :protocol)

    children = [
      :ranch.child_spec(
        :pg_wire,
        :ranch_tcp,
        [{:port, port}],
        PGWire.Connection,
        protocol: protocol
      )
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
