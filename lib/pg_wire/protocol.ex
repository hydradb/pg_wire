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
    row_description = ["id"] |> row_desc() |> encode_msg()
    row_value = ["hello"] |> data_row() |> encode_msg()

    command_complete =
      [tag: "SELECT 2"]
      |> msg_command_complete()
      |> encode_msg()
    query_ready =
      [status: ?I]
      |> msg_ready()
      |> encode_msg()

    {:multi_reply, [row_description, row_value, command_complete, query_ready], state}
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

  def data_row(row) do
    values = for field <- row, do: field_value(field)

    msg_data_row(values: values)
  end

  def row_desc(row) do
    fields = for field <- row, do: field_encode(field)

    msg_row_desc(fields: fields)
  end

  def field_value(value) do
    <<byte_size(value)::int32, value::binary>>
  end

  def field_encode(field) do
    <<
      field::binary,
      0::int8,
      0::int32,
      0::int16,
      17::int32,
      -1::int16,
      -1::int32,
      0::int16
    >>
  end
end

# {:ok, pid} = Postgrex.start_link(hostname: "localhost", username: "postgres", password: "postgres", database: "postgres", show_sensitive_data_on_connection_error: true)
