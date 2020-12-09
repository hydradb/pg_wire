defmodule PGWire.Protocol do
  import PGWire.BinaryUtils
  import PGWire.Messages

  def handle_message(msg_ssl_request(), state) do
    {:reply, <<?N>>, state}
  end

  def handle_message(msg_startup(params: _params), state) do
    msg =
      [type: auth_type(:cleartext)]
      |> msg_auth()
      |> encode_msg()

    {:reply, msg, state}
  end

  def handle_message(msg_password(pass: pass), state) do
    auth_ok =
      [type: auth_type(:ok)]
      |> msg_auth()
      |> encode_msg()

    query_ready =
      [status: ?I]
      |> msg_ready()
      |> encode_msg()

    {:multi_reply, [auth_ok, query_ready], state}
  end

  def handle_message(msg_query(statement: statement), state) do
    IO.puts("statement=#{inspect(statement)}")

    data = [
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

    row_d = data |> row_description() |> encode_msg()
    data_d = data |> row_data() |> Enum.map(&encode_msg/1)

    IO.puts("#{inspect(data_d)}")

    command_complete =
      [tag: "SELECT 2"]
      |> msg_command_complete()
      |> encode_msg()

    query_ready =
      [status: ?I]
      |> msg_ready()
      |> encode_msg()

    {:multi_reply, [row_d, data_d, command_complete, query_ready], state}
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

  #   RowDescription (B)
  #   Byte1('T')
  #   Identifies the message as a row description.
  #
  #   Int32
  #   Length of message contents in bytes, including self.
  #
  #   Int16
  #   Specifies the number of fields in a row (can be zero).
  #
  #   Then, for each field, there is the following:
  #
  #   String
  #   The field name.
  #
  #   Int32
  #   If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
  #
  #   Int16
  #   If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
  #
  #   Int32
  #   The object ID of the field's data type.
  #
  # Int16
  # The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
  #
  # Int32
  # The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
  #
  # Int16
  # The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
  # https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
  # https://segmentfault.com/a/1190000017136059
end

# {:ok, pid} = Postgrex.start_link(hostname: "localhost", username: "postgres", password: "postgres", database: "postgres", show_sensitive_data_on_connection_error: true)
