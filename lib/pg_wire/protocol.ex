defmodule PGWire.Protocol do
  @moduledoc false

  require PGWire.Messages
  alias PGWire.Messages

  @type message :: binary() | iolist()
  @type event :: :connected | :unauthenticated | :idle
  @type action ::
          {:next | :keep, message(), state :: term()}
          | {:disconnect | :error, reason :: term(), msgs :: message, state :: term()}

  @doc false
  @spec handle_message(Messages.t(), event(), term()) :: action()
  def handle_message(msg, current, state) when is_binary(msg) do
    case Messages.decode(msg) do
      :error -> {:error, {:unknown_msg, msg}, [], state}
      msg -> handle_message(msg, current, state)
    end
  end

  def handle_message(Messages.msg_terminate(), _, state), do: {:disconnect, :normal, [], state}

  def handle_message(Messages.msg_ssl_request(), :connected, state),
    do: {:keep, <<?N>>, state}

  def handle_message(Messages.msg_ssl_request(), _, state) do
    {:error, :normal, <<?E>>, state}
  end

  def handle_message(Messages.msg_startup(params: p), :connected, state) do
    auth_type = Messages.auth_type(:cleartext)

    msg =
      [type: auth_type]
      |> Messages.msg_auth()
      |> Messages.encode_msg()

    {:next, [msg], %{state | session_params: p}}
  end

  def handle_message(
        Messages.msg_password(pass: pass),
        :unauthenticated,
        %{session_params: params, mod: mod, state: mod_state} = state
      ) do
    opts =
      params
      |> Enum.into([])
      |> Kernel.++(password: pass, kind: :cleartext)

    req = struct(PGWire.Authentication, opts)

    case mod.handle_authentication(req, mod_state) do
      {:ok, _, new_state} ->
        msgs = auth_ok()
        {:next, msgs, %{state | state: new_state}}

      {not_ok, reason, msgs, new_state} when not_ok in [:error, :disconnect] ->
        {not_ok, reason, msgs, %{state | state: new_state}}
    end
  end

  def handle_message(
        Messages.msg_query(statement: stmt),
        :idle,
        %{mod: mod, state: mod_state} = state
      ) do
    tag = make_ref()
    query = %PGWire.Query{statement: stmt, tag: tag}

    case mod.handle_query(query, mod_state) do
      {:ok, msgs, new_state} ->
        {:keep, msgs, %{state | state: new_state}}

      {:error, reason, msgs, new_state} ->
        {:error, reason, msgs, %{state | state: new_state}}
    end
  end

  def handle_message({:"$notify", {topic, payload}}, :idle, state) do
    pg_pid = pid_to_int(self())

    msg =
      [pg_pid: pg_pid, channel: topic, payload: payload]
      |> Messages.msg_notify()
      |> Messages.encode_msg()

    {:keep, msg, state}
  end

  def handle_message(_msg, :idle, state) do
    {:keep, <<?E>>, state}
  end

  def handle_message(_msg, _state, data) do
    {:error, :normal, <<?E>>, data}
  end

  @spec encode_data([map()] | map()) :: iolist()
  def encode_data(rows) when is_list(rows) do
    for row <- rows, do: encode_data(row)
  end

  def encode_data(row) when is_map(row) do
    values = PGWire.Encoder.encode(row, [])

    [values: values]
    |> Messages.msg_data_row()
    |> Messages.encode_msg()
  end

  @spec encode_descriptor([map()] | map()) :: iolist()
  def encode_descriptor([row | _]) when is_map(row), do: encode_descriptor(row)

  def encode_descriptor(row) when is_map(row) do
    desc = PGWire.Descriptor.encode_descriptor(row, [])

    fields =
      for {name, %{oid: oid, typlen: typlen}} <- desc,
          do:
            Messages.row_field(
              name: name,
              table_oid: 0,
              column: 0,
              type_oid: oid,
              type_size: typlen,
              type_mod: -1,
              format: 0
            )

    [fields: fields]
    |> Messages.msg_row_desc()
    |> Messages.encode_msg()
  end

  @spec complete(PGWire.Query.t() | String.t(), integer()) :: iolist()
  def complete(%PGWire.Query{statement: statement}, affected), do: complete(statement, affected)

  def complete(statement, affected) do
    [command | _] = String.split(statement, " ")
    tag = command <> " " <> tag(command, affected)

    [tag: tag]
    |> Messages.msg_command_complete()
    |> Messages.encode_msg()
  end

  @spec ready() :: iolist()
  def ready do
    [status: ?I]
    |> Messages.msg_ready()
    |> Messages.encode_msg()
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

  def tag(command, affected) do
    case String.downcase(command) do
      "select" -> to_string(affected)
      "delete" -> to_string(affected)
      "update" -> to_string(affected)
      "fetch" -> to_string(affected)
      "copy" -> to_string(affected)
      "insert" -> "0 " <> to_string(affected)
      _ -> "0"
    end
  end

  defp pid_to_int(pid) do
    pid
    |> :erlang.pid_to_list()
    |> Enum.reduce(0, &Kernel.+/2)
  end
end
