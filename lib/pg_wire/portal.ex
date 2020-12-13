defmodule PGWire.Portal do
  use GenServer

  @type mode :: :simple | :extended

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  @spec query({term(), pid()}, Keyword.t()) :: Supervisor.on_start_child()
  def query({query, connection}, opts \\ []) do
    child_spec = child_spec([{query, connection, :simple}, opts])

    DynamicSupervisor.start_child(PGWire.PortalSupervisor, child_spec)
  end

  @spec start_link({term(), pid(), mode()}, Keyword.t()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @spec init(tuple()) :: {:ok, term()} | {:error, term()}
  def init({query, caller, mode}) do
    {:ok, %{}}
  end
end
