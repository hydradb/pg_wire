defmodule PGWire.Types do
  @spec typeof(term()) :: term()
  def typeof(term) do
    cond do
      is_nil(term) -> :null_value
      is_number(term) -> :number_value
      is_atom(term) -> :string_value
      is_boolean(term) -> {:bool}
      is_binary(term) -> :string_value
      is_list(term) -> :list_value
      is_map(term) -> :struct_value
      true -> raise ArgumentError, message: "Oops! got #{inspect(term)}"
    end
  end
end

defmodule PGWire.Encode do
  alias PGWire.Types.OID

  @type t :: {binary(), OID.t()}

  @spec string(binary()) :: t()
  def string(bin) when is_binary(bin), do: {<<bin::binary>>, OID.string()}

  @spec atom(atom()) :: t()
  def atom(atom) do
    atom
    |> to_string()
    |> string()
  end

  @spec integer(integer()) :: t()
  def integer(int) do
    {_oid, typelen} = o = OID.int()
    size = bin_size(typelen)
    int = Integer.to_string(int)
    {<<int::binary>>, o}
  end

  @spec float(float()) :: t()
  def float(f) when is_float(f) do
    o = OID.float()

    f = Float.to_string(f)
    {<<f::binary>>, o}
  end

  @spec map(map()) :: t()
  def map(map) do
    o = OID.json_t()
    json = Jason.encode!(map)

    {<<json::binary>>, o}
  end

  @spec list(list()) :: t()
  def list(list) do
    o = OID.json_t()
    json = Jason.encode!(list)

    {<<json::binary>>, o}
  end

  defp bin_size(typelen), do: typelen * 8
end

defprotocol PGWire.Encoder do
  @type t :: term()
  @type opts :: Keyword.t()

  @spec encode(t(), opts) :: term()
  def encode(row, opts)
end

defprotocol PGWire.Descriptor do
  @type t :: term()
  @type opts :: Keyword.t()

  @spec encode_descriptor(t(), opts) :: term()
  def encode_descriptor(row, opts)
end

defimpl PGWire.Encoder, for: Integer do
  def encode(value, _opts \\ []) do
    PGWire.Encode.integer(value)
  end
end

defimpl PGWire.Encoder, for: Float do
  def encode(value, _opts \\ []) do
    PGWire.Encode.float(value)
  end
end

defimpl PGWire.Encoder, for: List do
  def encode(value, _opts \\ []) do
    PGWire.Encode.list(value)
  end
end

defimpl PGWire.Encoder, for: BitString do
  def encode(value, _opts \\ []) do
    PGWire.Encode.string(value)
  end
end

defimpl PGWire.Encoder, for: Map do
  def encode(map, opts) do
    _keys = Keyword.get(opts, :descriptor, Map.keys(map))

    map
    |> Map.values()
    |> Enum.map(&(&1 |> do_encode() |> elem(0)))
  end

  defp do_encode(map) when is_map(map), do: PGWire.Encode.map(map)
  defp do_encode(list) when is_list(list), do: PGWire.Encode.list(list)
  defp do_encode(value), do: PGWire.Encoder.encode(value, [])
end

defimpl PGWire.Descriptor, for: Map do
  def encode_descriptor(map, _opts) do
    # Enum.map(map, &(&1 |> do_encode() |> elem(1)))
    Enum.map(map, fn {k, v} ->
      {_, desc} = do_encode(v)
      {k, desc}
    end)
  end

  defp do_encode(map) when is_map(map), do: PGWire.Encode.map(map)
  defp do_encode(list) when is_list(list), do: PGWire.Encode.list(list)
  defp do_encode(value), do: PGWire.Encoder.encode(value, [])
end
