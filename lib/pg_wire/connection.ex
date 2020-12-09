defmodule PGWire.Connection do
  @behaviour :gen_statem

  @timeout 1000 * 60 * 60
  @test_data [
    %{
      "id" => 1,
      "name" => "Hydra DB",
      "map" => %{"key" => "value"},
      "list" => [1, 2, 3],
      "float" => 1.0
    },
    %{
      "id" => 2,
      "name" => "Hydra DB",
      "map" => %{"key" => "value"},
      "list" => [4, 5, 6],
      "float" => 2.0
    }
  ]

  require PGWire.Messages
  require Logger

  alias PGWire.Messages

  defstruct [:socket, :transport]

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
    {msg, next_state} =
      case Messages.decode(msg) do
        Messages.msg_ssl_request() ->
          {<<?N>>, :connected}

        Messages.msg_startup(params: _p) ->
          auth_type = Messages.auth_type(:cleartext)

          msg =
            [type: auth_type]
            |> Messages.msg_auth()
            |> Messages.encode_msg()

          {msg, :unauthenticated}

        msg ->
          Logger.warn(fn -> "#{inspect(msg)} not valid in state=connected" end)
          {<<"Error"::binary>>, :disconnect}
      end

    transport.send(socket, msg)

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
      case Messages.decode(msg) do
        Messages.msg_password(pass: _pass) ->
          auth_type = Messages.auth_type(:ok)

          auth_ok =
            [type: auth_type]
            |> Messages.msg_auth()
            |> Messages.encode_msg()

          q_ready =
            [status: ?I]
            |> Messages.msg_ready()
            |> Messages.encode_msg()

          {[auth_ok, q_ready], :idle}

        msg ->
          Logger.warn(fn -> "#{inspect(msg)} not valid in state=connected" end)
          {<<"Error"::binary>>, :disconnect}
      end

    for msg <- List.wrap(msgs), do: transport.send(socket, msg)

    {:next_state, next_state, state, []}
  end

  def unauthenticated(:info, {:tcp_closed, _}, _data) do
    :keep_state_and_data
  end

  def idle(:info, {:tcp, _, msg}, %__MODULE__{transport: transport, socket: socket} = state) do
    {msgs, next_state} =
      case Messages.decode(msg) do
        Messages.msg_query(statement: statement) ->
          Logger.debug(fn -> "Received Query=#{statement}" end)

          row_d =
            @test_data
            |> row_description()
            |> Messages.encode_msg()

          data_d =
            @test_data
            |> row_data()
            |> Enum.map(&Messages.encode_msg/1)

          command_complete =
            [tag: "SELECT 2"]
            |> Messages.msg_command_complete()
            |> Messages.encode_msg()

          query_ready =
            [status: ?I]
            |> Messages.msg_ready()
            |> Messages.encode_msg()

          {[row_d, data_d, command_complete, query_ready], :idle}

        msg ->
          Logger.warn(fn -> "#{inspect(msg)} not valid in state=connected" end)
          {<<"Error"::binary>>, :disconnect}
      end

    for msg <- List.wrap(msgs), do: transport.send(socket, msg)

    {:next_state, next_state, state, []}
  end

  def disconnect(_, _, _) do
    {:stop, :normal}
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
