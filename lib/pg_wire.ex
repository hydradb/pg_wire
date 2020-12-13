defmodule PGWire do
  @type opts :: Keyword.t()

  @spec child_spec(opts) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  @spec start_link(PGWire.Protocol.t(), opts) :: Supervisor.on_start()
  def start_link(protocol, opts \\ []) do
    opts = Keyword.merge(opts, protocol: protocol)

    PGWire.Supervisor.start_link(opts)
  end
end
