defmodule PGEcho do
  use Application

  def start(_type, _args) do
    children = [{PGWire, [PGEcho.Protocol]}]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
