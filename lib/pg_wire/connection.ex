defmodule PGWire.Connection do
  @behaviour :gen_statem

  require Logger
  alias PGWire.Protocol

  defstruct [
    :socket,
    :transport,
    :portals,
    :session_params,
    :mod,
    :state
  ]

  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [{ref, transport, opts}])
    {:ok, pid}
  end

  @impl true
  def callback_mode(), do: :state_functions

  @impl true
  def init({ref, transport, opts}) do
    {mod, opts} = Keyword.pop!(opts, :protocol)
    {:ok, socket} = :ranch.handshake(ref)

    :ok =
      transport.setopts(socket,
        active: :once,
        nodelay: true,
        reuseaddr: true
      )

    case mod.init(opts) do
      {:ok, state} -> init_state(transport, socket, mod, state)
      {:stop, _} = stop -> stop
      :ignore -> :ignore
      other -> {:stop, {:bad_return_value, other}}
    end
  end

  @impl true
  def terminate(_reason, _state, %__MODULE__{transport: transport, socket: socket}) do
    transport.close(socket)
  end

  def connected(:info, {:tcp_error, _, reason}, _state), do: {:stop, reason}
  def connected(:info, {:tcp_closed, _}, _state), do: {:stop, :normal}

  def connected(:info, {:tcp, _, msg}, data) do
    handle_message(msg, :connected, :unauthenticated, data)
  end

  def unauthenticated(:info, {:tcp_error, _, reason}, _state), do: {:stop, reason}
  def unauthenticated(:info, {:tcp_closed, _}, _state), do: {:stop, :normal}

  def unauthenticated(:info, {:tcp, _, msg}, data) do
    handle_message(msg, :unauthenticated, :idle, data)
  end

  def idle(:info, {:tcp_error, _, reason}, _state), do: {:stop, reason}
  def idle(:info, {:tcp_closed, _}, _state), do: {:stop, :normal}

  def idle(:info, {:tcp, _, msg}, data) do
    handle_message(msg, :idle, :keep_state, data)
  end

  def idle(:info, {:"$notify", _} = msg, data) do
    handle_message(msg, :idle, :keep_state, data)
  end

  def idle(:info, message, %__MODULE__{state: state} = data),
    do: noreply_callback(:handle_info, {message, state}, data)

  def disconnect(_, msg, %__MODULE__{transport: transport, socket: socket} = data) do
    Logger.info(fn -> "Socket disconnect msg=#{inspect(msg)}" end)

    :ok = transport.close(socket)
    {:stop, :normal, data}
  end

  defp handle_message(msg, current_state, next_state, %__MODULE__{} = data) do
    {state_transition, msgs} =
      case Protocol.handle_message(msg, current_state, data) do
        {:next, msgs, new_data} ->
          {{:next_state, next_state, setopts(new_data, {:active, :once}), []}, msgs}

        {:keep, msgs, new_data} ->
          {{:keep_state, setopts(new_data, {:active, :once}), []}, msgs}

        {:disconnect, reason, msgs, new_data} ->
          action = {:next_event, :cast, reason}
          {{:next_state, :disconnect, new_data, [action]}, msgs}

        {:error, reason, msgs, new_data} ->
          action = {:next_event, :cast, reason}
          {{:next_state, :disconnect, new_data, [action]}, msgs}
      end

    _ = send_reply(msgs, data)
    state_transition
  end

  defp send_reply(msgs, %__MODULE__{transport: transport, socket: socket}) do
    transport.send(socket, List.wrap(msgs))
  end

  defp init_state(transport, socket, mod, state) do
    data = %__MODULE__{
      socket: socket,
      transport: transport,
      session_params: %{},
      mod: mod,
      state: state,
      portals: %{}
    }

    :gen_statem.enter_loop(__MODULE__, [], :connected, data)
  end

  defp noreply_callback(:handle_info, {msg, state}, %{mod: mod} = data) do
    if function_exported?(mod, :handle_info, 2) do
      case mod.handle_info(msg, state) do
        {:noreply, msgs, state} ->
          send_reply(msgs, data)
          {:keep_state, %{data | state: state}, []}

        {:stop, reason, state} ->
          {:stop, reason, %{data | state: state}}

        other ->
          {:stop, {:bad_return_value, other}, data}
      end
    else
      log = '** Undefined handle_info in ~tp~n** Unhandled message: ~tp~n'
      :error_logger.warning_msg(log, [mod, msg])
      {:noreply, %{data | state: state}}
    end
  end

  defp setopts(%{transport: transport, socket: socket} = data, opts) do
    transport.setopts(socket, List.wrap(opts))
    data
  end
end
