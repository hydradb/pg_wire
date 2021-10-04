defmodule PGWire.Encode do
  alias PGWire.Catalog.Types

  @type t :: {binary(), OID.t()}

  @spec string(binary()) :: t()
  def string(bin) when is_binary(bin), do: {<<bin::binary>>, Types.type_info(:text)}

  @spec atom(atom()) :: t()
  def atom(nil) do
    {<<"NULL"::binary>>, %{}}
  end

  @spec atom(atom()) :: t()
  def atom(true) do
    o = Types.type_info(:bool)
    {<<"true"::binary>>, o}
  end

  @spec atom(atom()) :: t()
  def atom(false) do
    o = Types.type_info(:bool)
    {<<"false"::binary>>, o}
  end

  @spec atom(atom()) :: t()
  def atom(atom) do
    atom
    |> to_string()
    |> string()
  end

  @spec integer(integer()) :: t()
  def integer(int) do
    %{typlen: typlen} = o = Types.type_info(:int8)
    int = Integer.to_string(int)
    {<<int::binary>>, o}
  end

  @spec float(float()) :: t()
  def float(f) when is_float(f) do
    o = Types.type_info(:float8)

    f = Float.to_string(f)
    {<<f::binary>>, o}
  end

  @spec map(map()) :: t()
  def map(map) do
    o = Types.type_info(:json)
    json = Jason.encode!(map)

    {<<json::binary>>, o}
  end

  @spec list(list()) :: t()
  def list(list) do
    o = Types.type_info(:json)
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

defimpl PGWire.Encoder, for: Atom do
  def encode(value, _opts \\ []) do
    PGWire.Encode.atom(value)
  end
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
  defp do_encode(nil), do: PGWire.Encoder.encode("", [])
  defp do_encode(value), do: PGWire.Encoder.encode(value, [])
end
