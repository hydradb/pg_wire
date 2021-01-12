defmodule PGWire.Protocol do
  defmodule Error do
    require PGWire.Messages
    alias PGWire.{Messages, Error}

    @spec fatal(atom(), Keyword.t()) :: iolist()
    def fatal(error_name, opts \\ []) do
      error_name
      |> Error.fatal(opts)
      |> encode_error()
    end

    @spec error(atom(), Keyword.t()) :: iolist()
    def error(error_name, opts \\ []) do
      error_name
      |> Error.error(opts)
      |> encode_error()
    end

    @spec warning(atom(), Keyword.t()) :: iolist()
    def warning(error_name, opts \\ []) do
      error_name
      |> Error.warning(opts)
      |> encode_error()
    end

    defp encode_error(error) do
      error_fields = Error.to_map(error)

      [fields: error_fields]
      |> Messages.msg_error()
      |> Messages.encode_msg()
    end
  end

  require PGWire.Messages
  alias PGWire.Messages

  @callback init(args :: term) ::
              {:ok, state}
              | :ignore
              | {:stop, reason :: any}
            when state: any

  @callback handle_authentication(auth :: any(), state :: term) ::
              {:ok, [message], new_state}
              | {:error, reason, new_state}
              | {status, new_state}
              | {:disconnect, reason, new_state}
            when new_state: term, reason: term, message: term, status: term

  @callback handle_query(query :: any(), state :: term) ::
              {:ok, [message], new_state}
              | {status, new_state}
              | {:disconnect, reason, new_state}
            when new_state: term, reason: term, message: term, status: term

  defmacro __using__(_opts \\ []) do
    quote location: :keep do
      @behaviour PGWire.Protocol
      import PGWire.Protocol,
        only: [
          encode_data: 1,
          encode_descriptor: 1,
          complete: 2,
          ready: 0
        ]
    end
  end

  @doc false
  def handle_message(msg, current, state) when is_binary(msg) do
    case Messages.decode(msg) do
      :error -> {:error, {:unknown_msg, msg}, []}
      msg -> handle_message(msg, current, state)
    end
  end

  def handle_message(Messages.msg_terminate(), _, state), do: {:disconnect, :normal, [], state}

  def handle_message(Messages.msg_ssl_request(), :connected, state),
    do: {:keep, <<?N>>, state}

  def handle_message(Messages.msg_ssl_request(), _, state), do: {:error, <<?E>>, state}

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

      {:error, msgs, new_state} ->
        {:error, msgs, %{state | state: new_state}}
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

      {:error, msgs, new_state} ->
        {:error, msgs, %{state | state: new_state}}
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
    {:error, <<?E>>, data}
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
