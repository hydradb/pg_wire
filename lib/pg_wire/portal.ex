defmodule PGWire.Portal do
  use GenServer

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  def query({query, connection}, opts \\ []) do
    child_spec = child_spec([{query, connection, :simple}, opts])

    DynamicSupervisor.start_child(PGWire.PortalSupervisor, child_spec)
  end

  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  def init(_) do
    {:ok, %{}}
  end
end
