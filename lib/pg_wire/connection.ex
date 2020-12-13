defmodule PGWire.Connection do
  @behaviour :gen_statem

  require PGWire.Messages
  require Logger

  alias PGWire.Messages

  defstruct [:socket, :transport, :portals, :session_params, :mod, :mod_state]

  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [{ref, transport, opts}])
    {:ok, pid}
  end

  @impl true
  def callback_mode(), do: :state_functions

  @impl true
  def init({ref, transport, opts}) do
    {:ok, mod} = Keyword.fetch(opts, :protocol)
    {:ok, socket} = :ranch.handshake(ref)

    :ok =
      transport.setopts(socket,
        active: true,
        nodelay: true,
        reuseaddr: true
      )

    {:ok, mod_state} = mod.init(opts)

    state = %__MODULE__{
      socket: socket,
      transport: transport,
      session_params: %{},
      mod: mod,
      mod_state: mod_state,
      portals: %{}
    }

    :gen_statem.enter_loop(__MODULE__, [], :connected, state)
  end

  @impl true
  def terminate(reason, _state, data) do
    data.transport.close(data.socket)
  end

  def connected(:info, {:tcp, _, msg}, %__MODULE__{transport: transport, socket: socket} = state) do
    {msgs, next_state, new_data_state} =
      msg
      |> Messages.decode()
      |> handle_startup(state)

    transport.send(socket, msgs)

    {:next_state, next_state, new_data_state, []}
  end

  def connected(:info, {:tcp_error, _, reason}, _state) do
    {:stop, reason}
  end

  def unauthenticated(
        :info,
        {:tcp, _, msg},
        %__MODULE__{transport: transport, socket: socket} = state
      ) do
    {msgs, next_state, new_data_state} =
      msg
      |> Messages.decode()
      |> handle_authenticate(state)

    for msg <- List.wrap(msgs), do: transport.send(socket, msg)
    {:next_state, next_state, new_data_state, []}
  end

  def unauthenticated(:info, {:tcp_closed, _}, _data) do
    :keep_state_and_data
  end

  def idle(
        :info,
        {:tcp, _, msg},
        %__MODULE__{transport: transport, socket: socket, portals: ps} = state
      ) do
    maybe_portal_started =
      msg
      |> Messages.decode()
      |> handle_query()

    new_state =
      case maybe_portal_started do
        {:ok, {ref, query}} ->
          %{state | portals: Map.put(ps, ref, query)}

        {:error, msgs} ->
          for msg <- List.wrap(msgs), do: transport.send(socket, msg)
          state
      end

    {:keep_state, new_state, []}
  end

  def disconnect(_, _, _) do
    {:stop, :normal}
  end

  def handle_startup(Messages.msg_ssl_request(), state) do
    {<<?N>>, :connected, state}
  end

  def handle_startup(Messages.msg_startup(params: p), state) do
    auth_type = Messages.auth_type(:cleartext)

    msg =
      [type: auth_type]
      |> Messages.msg_auth()
      |> Messages.encode_msg()

    {msg, :unauthenticated, %{state | session_params: p}}
  end

  def handle_startup(msg, state) do
    {<<"Error #{inspect(msg)}"::binary>>, :disconnect, state}
  end

  def handle_authenticate(Messages.msg_password(pass: _pass), state) do
    auth_type = Messages.auth_type(:ok)

    auth_ok =
      [type: auth_type]
      |> Messages.msg_auth()
      |> Messages.encode_msg()

    q_ready =
      [status: ?I]
      |> Messages.msg_ready()
      |> Messages.encode_msg()

    {[auth_ok, q_ready], :idle, state}
  end

  def handle_authenticate(msg, state) do
    {<<"Error #{inspect(msg)}"::binary>>, :disconnect, state}
  end

  def handle_query(Messages.msg_query() = q) do
    {:ok, pid} = PGWire.Portal.query({q, self()})
    ref = Process.monitor(pid)

    {:ok, {ref, q}}
  end

  def handle_query(msg) do
    {:error, <<?E>>}
  end

  defp row_data(rows) when is_list(rows) do
    for row <- rows, do: row_data(row)
  end

  defp row_data(row) when is_map(row) do
    values = PGWire.Encoder.encode(row, [])
    Messages.msg_data_row(values: values)
  end

  defp row_description([row | _]) when is_map(row), do: row_description(row)

  defp row_description(row) when is_map(row) do
    desc = PGWire.Descriptor.encode_descriptor(row, [])
    Messages.msg_row_desc(fields: desc)
  end
end
