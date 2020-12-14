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
    Logger.debug("#{__MODULE__} :: Entering idle mode ....")

    maybe_portal_started =
      msg
      |> Messages.decode()
      |> handle_query(state)

    new_state =
      case maybe_portal_started do
        {:ok, {ref, query}} ->
          Logger.debug("#{__MODULE__} :: [idle] portal started ....")
          %{state | portals: Map.put(ps, ref, query)}

        {:error, msgs} ->
          Logger.error("#{__MODULE__} :: [idle] error starting portal #{inspect(msgs)} ....")
          for msg <- List.wrap(msgs), do: transport.send(socket, msg)
          state
      end

    {:keep_state, new_state, []}
  end

  def idle(
        {:call, from},
        {:complete, ref, msgs},
        %__MODULE__{transport: transport, socket: socket, portals: portals} = state
      ) do
    query_ready =
      [status: ?I]
      |> Messages.msg_ready()
      |> Messages.encode_msg()

    for msg <- List.wrap(msgs ++ [query_ready]), do: transport.send(socket, msg)
    {_, portals} = Map.pop!(portals, ref)

    Logger.debug("#{__MODULE__} :: [idle::__call__:complete] ....")

    {:keep_state, %{state | portals: portals}, [{:reply, from, :ok}]}
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

  def handle_authenticate(
        Messages.msg_password(pass: pass),
        %{session_params: params, mod: mod, mod_state: mod_state} = state
      ) do
    opts =
      params
      |> Enum.into([])
      |> Kernel.++(password: pass, kind: :cleartext)

    auth_req = PGWire.Auth.new(opts)

    case mod.handle_authenticate(auth_req, mod_state) do
      {:ok, mod_state} ->
        msgs = auth_ok()
        {msgs, :idle, %{state | mod_state: mod_state}}

      {:error, _reason} ->
        {<<?E>>, :disconnect, state}
    end
  end

  def handle_authenticate(msg, state) do
    {<<"Error #{inspect(msg)}"::binary>>, :disconnect, state}
  end

  def handle_query(Messages.msg_query(statement: statement) = q, %{mod: mod, mod_state: mod_state}) do
    tag = make_ref()
    query = %PGWire.Query{statement: statement, tag: tag}
    callback = fn from, msg -> :gen_statem.call(from, msg) end

    with {:ok, pid} <- PGWire.Portal.start_portal({mod, mod_state}),
         :ok <- PGWire.Portal.query(pid, query, callback) do
      # TODO maybe monitor
      {:ok, {tag, q}}
    end
  end

  def handle_query(msg, state) do
    {:error, <<?E>>}
  end

  defp auth_ok do
    auth_type = Messages.auth_type(:ok)

    ok =
      [type: auth_type]
      |> Messages.msg_auth()
      |> Messages.encode_msg()

    ready =
      [status: ?I]
      |> Messages.msg_ready()
      |> Messages.encode_msg()

    [ok, ready]
  end
end
