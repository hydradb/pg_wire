defmodule PGWire.Connection do
  @behaviour :gen_statem

  require PGWire.Messages
  require Logger

  alias PGWire.Messages

  defstruct [:socket, :transport, :portal]

  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [{ref, transport, opts}])
    {:ok, pid}
  end

  @impl true
  def callback_mode(), do: :state_functions

  @impl true
  def init({ref, transport, _opts}) do
    {:ok, socket} = :ranch.handshake(ref)

    :ok =
      transport.setopts(socket,
        active: true,
        nodelay: true,
        reuseaddr: true
      )

    :gen_statem.enter_loop(__MODULE__, [], :connected, %__MODULE__{
      socket: socket,
      transport: transport
    })
  end

  @impl true
  def terminate(reason, _state, data) do
    data.transport.close(data.socket)
  end

  def connected(:info, {:tcp, _, msg}, %__MODULE__{transport: transport, socket: socket} = state) do
    {msgs, next_state} =
      msg
      |> Messages.decode()
      |> PGWire.Protocol.startup()

    transport.send(socket, msgs)

    {:next_state, next_state, state, []}
  end

  def connected(:info, {:tcp_error, _, reason}, _state) do
    {:stop, reason}
  end

  def unauthenticated(
        :info,
        {:tcp, _, msg},
        %__MODULE__{transport: transport, socket: socket} = state
      ) do
    {msgs, next_state} =
      msg
      |> Messages.decode()
      |> PGWire.Protocol.authenticate()

    for msg <- List.wrap(msgs), do: transport.send(socket, msg)

    {:next_state, next_state, state, []}
  end

  def unauthenticated(:info, {:tcp_closed, _}, _data) do
    :keep_state_and_data
  end

  def idle(:info, {:tcp, _, msg}, %__MODULE__{transport: transport, socket: socket} = state) do
    {msgs, next_state} =
      msg
      |> Messages.decode()
      |> PGWire.Protocol.query()

    for msg <- List.wrap(msgs), do: transport.send(socket, msg)

    {:next_state, next_state, state, []}
  end

  def disconnect(_, _, _) do
    {:stop, :normal}
  end
end
