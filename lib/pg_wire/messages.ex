defmodule PGWire.Messages do
  import Record, only: [defrecord: 2]
  import PGWire.BinaryUtils
  @protocol_vsn_major 3
  @protocol_vsn_minor 0

  @auth_types [
    ok: 0,
    kerberos: 2,
    cleartext: 3,
    md5: 5,
    scm: 6,
    gss: 7,
    gss_cont: 8,
    sspi: 9,
    sasl: 10,
    sasl_cont: 11,
    sasl_fin: 12
  ]

  @error_fields [
    severity: ?S,
    code: ?C,
    message: ?M,
    detail: ?D,
    hint: ?H,
    position: ?P,
    internal_position: ?p,
    internal_query: ?q,
    where: ?W,
    schema: ?s,
    table: ?t,
    column: ?c,
    data_type: ?d,
    constraint: ?n,
    file: ?F,
    line: ?L,
    routine: ?R
  ]

  defrecord :msg_auth, [:type]
  defrecord :msg_startup, [:params]
  defrecord :msg_password, [:pass]
  defrecord :msg_error, [:fields]
  defrecord :msg_parameter, [:name, :value]
  defrecord :msg_backend_key, [:pid, :key]
  defrecord :msg_ready, [:status]
  defrecord :msg_notice, [:fields]
  defrecord :msg_query, [:statement]
  defrecord :msg_parse, [:name, :statement, :type_oids]
  defrecord :msg_describe, [:type, :name]
  defrecord :msg_flush, []
  defrecord :msg_close, [:type, :name]
  defrecord :msg_parse_complete, []
  defrecord :msg_parameter_desc, [:type_oids]
  defrecord :msg_too_many_parameters, [:len, :max_len]
  defrecord :msg_row_desc, [:fields]
  defrecord :msg_no_data, []
  defrecord :msg_notify, [:pg_pid, :channel, :payload]
  defrecord :msg_bind, [:name_port, :name_stat, :param_formats, :params, :result_formats]
  defrecord :msg_execute, [:name_port, :max_rows]
  defrecord :msg_sync, []
  defrecord :msg_bind_complete, []
  defrecord :msg_close_complete, []
  defrecord :msg_portal_suspend, []
  defrecord :msg_data_row, [:values]
  defrecord :msg_command_complete, [:tag]
  defrecord :msg_empty_query, []
  defrecord :msg_copy_data, [:data]
  defrecord :msg_copy_done, []
  defrecord :msg_copy_fail, [:message]
  defrecord :msg_copy_in_response, [:format, :columns]
  defrecord :msg_copy_both_response, [:format, :columns]
  defrecord :msg_copy_out_response, [:format, :columns]
  defrecord :msg_terminate, []
  defrecord :msg_ssl_request, []
  defrecord :msg_cancel_request, [:pid, :key]
  defrecord :row_field, [:name, :table_oid, :column, :type_oid, :type_size, :type_mod, :format]

  def decode(<<length::int32, 1234::int16, 5679::int16>>) do
    msg_ssl_request()
  end

  def decode(
        <<_length::int32, @protocol_vsn_major::int16, @protocol_vsn_minor::int16, rest::binary>>
      ) do
    params = decode_params(rest, %{})

    msg_startup(params: params)
  end

  def decode(<<?p, _length::int32, pass::binary>>) do
    msg_password(pass: pass)
  end

  def decode(<<?Q, _length::int32, statement::binary>>) do
    # <<0>> following strings so cut it
    statement = :binary.part(statement, 0, byte_size(statement) - 1)
    msg_query(statement: statement)
  end

  def decode(msg) do
    IO.puts("Unknown message #{inspect(msg)}")
    msg
  end

  def decode_params(<<>>, acc), do: acc
  def decode_params(<<0>>, acc), do: acc

  def decode_params(<<"user"::binary, 0::int8, rest::binary>>, acc),
    do: do_decode_params(:user, rest, acc)

  def decode_params(<<"database"::binary, 0::int8, rest::binary>>, acc),
    do: do_decode_params(:database, rest, acc)

  def decode_params(<<"options"::binary, 0::int8, rest::binary>>, acc),
    do: do_decode_params(:options, rest, acc)

  def decode_params(<<_flags::binary>>, acc), do: acc

  defp do_decode_params(key, rest, acc) do
    {value, binary} = decode_value(rest)
    acc = Map.put(acc, key, value)

    decode_params(binary, acc)
  end

  def decode_value(<<rest::binary>>) do
    value =
      rest
      |> to_charlist()
      |> Enum.take_while(&(&1 != 0))

    size = byte_size(rest)
    pos_len = {size, -(size - length(value)) + 1}
    slice = :binary.part(rest, pos_len)

    {to_string(value), slice}
  end

  # defp decode_params(<<params::binary>>) do
  #   case params do
  #     <<key::binary, 0::int8, value::binary, 0::int8, rest::binary>>
  #   end
  # end

  def encode(msg_auth(type: type)) do
    {<<?R>>, <<type::int32>>}
  end

  def encode(msg_ready(status: status)) do
    {<<?Z>>, <<status::int8>>}
  end

  def encode(msg_row_desc(fields: fields)) do
    bin =
      Enum.reduce(fields, <<length(fields)::int16>>, fn {key, {oid, typelen}}, acc ->
        b =
          <<key::binary, 0::int8, 0::int32, 0::int16, oid::int32, typelen::int16, -1::int32,
            1::int16>>

        [acc, b]
      end)

    {<<?T>>, bin}
  end

  def encode(msg_data_row(values: values)) do
    bin =
      Enum.reduce(values, <<length(values)::int16>>, fn value, acc ->
        b = <<byte_size(value)::int32, value::binary>>
        [acc, b]
      end)

    {<<?D>>, bin}
  end

  def encode(msg_command_complete(tag: tag)) do
    {<<?C>>, <<tag::binary, 0::int8>>}
  end

  def encode_msg(msg) do
    {first, data} = encode(msg)
    size = IO.iodata_length(data) + 4

    if first do
      [first, <<size::int32>>, data]
    else
      [<<size::int32>>, data]
    end
  end

  def encode do
    params = [user: "simon", database: "hydra"]

    params =
      Enum.reduce(params, [], fn {key, value}, acc ->
        [acc, to_string(key), 0, value, 0]
      end)

    vsn = <<@protocol_vsn_major::int16, @protocol_vsn_minor::int16>>
    data = [vsn, params, 0] |> :erlang.iolist_to_binary()
    size = IO.iodata_length(data) + 4
    <<size::int32>> <> data
  end

  def auth_type(kind), do: Keyword.fetch!(@auth_types, kind)
end
