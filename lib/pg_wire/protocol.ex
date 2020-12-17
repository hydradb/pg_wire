defmodule PGWire.Protocol do
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

  def handle_message(_msg, :idle, state) do
    {:keep, <<?E>>, state}
  end

  def handle_message(_msg, _state, data) do
    {:error, <<?E>>, data}
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

    [fields: desc]
    |> Messages.msg_row_desc()
    |> Messages.encode_msg()
  end

  @spec complete(PGWire.Query.t() | String.t(), integer()) :: iolist()
  def complete(%PGWire.Query{statement: statement}, affected), do: complete(statement, affected)

  def complete(statement, affected) do
    [command | _] = String.split(statement, " ")
    tag = command <> " " <> to_string(affected)

    [tag: tag]
    |> Messages.msg_command_complete()
    |> Messages.encode_msg()
  end

  def ready do
    [status: ?I]
    |> Messages.msg_ready()
    |> Messages.encode_msg()
  end
end
