defmodule PGEcho do
  use Application

  def start(_type, _args) do
    children = [{PGWire, [PGEcho.Handler, [port: 5455]]}]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
