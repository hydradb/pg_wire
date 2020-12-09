defmodule PGWire.Protocol do
  import PGWire.Messages

  @data [
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

  def startup(msg_ssl_request()) do
    {<<?N>>, :connected}
  end

  def startup(msg_startup(params: _p)) do
    auth_type = auth_type(:cleartext)

    msg =
      [type: auth_type]
      |> msg_auth()
      |> encode_msg()

    {msg, :unauthenticated}
  end

  def startup(msg) do
    {<<"Error #{inspect(msg)}"::binary>>, :disconnect}
  end

  def authenticate(msg_password(pass: _pass)) do
    auth_type = auth_type(:ok)

    auth_ok =
      [type: auth_type]
      |> msg_auth()
      |> encode_msg()

    q_ready =
      [status: ?I]
      |> msg_ready()
      |> encode_msg()

    {[auth_ok, q_ready], :idle}
  end

  def authenticate(msg) do
    {<<"Error #{inspect(msg)}"::binary>>, :disconnect}
  end

  def query(msg_query(statement: statement)) do
    row_d =
      @data
      |> row_description()
      |> encode_msg()

    data_d =
      @data
      |> row_data()
      |> Enum.map(&encode_msg/1)

    command_complete =
      [tag: "SELECT 2"]
      |> msg_command_complete()
      |> encode_msg()

    query_ready =
      [status: ?I]
      |> msg_ready()
      |> encode_msg()

    {[row_d, data_d, command_complete, query_ready], :idle}
  end

  def query(msg) do
    {<<"Error #{inspect(msg)}"::binary>>, :disconnect}
  end

  defp row_data(rows) when is_list(rows) do
    for row <- rows, do: row_data(row)
  end

  defp row_data(row) when is_map(row) do
    values = PGWire.Encoder.encode(row, [])
    msg_data_row(values: values)
  end

  defp row_description([row | _]) when is_map(row), do: row_description(row)

  defp row_description(row) when is_map(row) do
    desc = PGWire.Descriptor.encode_descriptor(row, [])
    msg_row_desc(fields: desc)
  end
end
