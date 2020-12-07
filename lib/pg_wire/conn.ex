defmodule PGWire.Conn do
  use GenServer
  @behaviour :ranch_protocol
  require Logger

  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [{ref, transport, opts}])
    {:ok, pid}
  end

  def init({ref, transport, _opts}) do
    {:ok, socket} = :ranch.handshake(ref)

    :ok =
      transport.setopts(socket,
        active: true,
        nodelay: true,
        reuseaddr: true
      )

    :gen_server.enter_loop(__MODULE__, [], %{
      socket: socket,
      transport: transport
    })
  end

  def handle_info({:tcp, unknown, data}, %{transport: transport, socket: s} = state) do
    maybe_reply =
      data
      |> PGWire.Messages.decode()
      |> PGWire.Protocol.handle_message(state)

    new_state =
      case maybe_reply do
        {:reply, msg, new_state} ->
          transport.send(s, msg)
          new_state

        {:multi_reply, msgs, new_state} ->
          Enum.each(msgs, &transport.send(s, &1))
          new_state

        {:error, reason} ->
          Logger.warn(fn -> "Message decode error #{inspect(data)} reason=#{inspect(reason)}" end)
          state

        {:no_reply, new_state} ->
          new_state
      end

    {:noreply, new_state}
  end

  def handle_info({:tcp_error, unknown, reason}, state) do
    Logger.error(fn -> ":tcp_error unknown=#{unknown} reason=#{reason}" end)
    {:stop, :normal, state}
  end

  def handle_info({:tcp_closed, unknown}, state) do
    Logger.info(fn -> ":tcp_closed #{inspect(unknown)}" end)

    {:stop, :normal, state}
  end

  def terminate(reason, state) do
    Logger.info("terminated reason #{inspect(reason)} #{inspect(state)}")
    :ok
  end

  defp stringify_clientname(socket) do
    {:ok, {addr, port}} = :inet.peername(socket)

    address =
      case addr do
        :local ->
          "UNIX-SOCKET://"

        addr ->
          addr
          |> :inet_parse.ntoa()
          |> to_string()
      end

    "#{address}:#{port}"
  end
end
