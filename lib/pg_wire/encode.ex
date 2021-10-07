defmodule PGWire.Encode do
  @type t :: {binary(), PGWire.Catalog.Types.OID.t()}

  @callback string(binary()) :: t()
  @callback atom(atom()) :: t()
  @callback integer(integer()) :: t()
  @callback float(float()) :: t()
  @callback map(map()) :: t()
  @callback list(list()) :: t()
end

defprotocol PGWire.Encoder do
  @type t :: term()
  @type opts :: Keyword.t()

  @spec encode(t(), opts) :: term()
  def encode(row, opts \\ [])
end

defprotocol PGWire.Descriptor do
  @type t :: term()
  @type opts :: Keyword.t()

  @spec encode_descriptor(t(), opts) :: term()
  def encode_descriptor(row, opts)
end

defimpl PGWire.Encoder, for: Atom do
  def encode(value, opts) do
    encoder = Keyword.get(opts, :encoder, PGWire.Encoder.Text)
    encoder.atom(value)
  end
end

defimpl PGWire.Encoder, for: Integer do
  def encode(value, opts) do
    encoder = Keyword.get(opts, :encoder, PGWire.Encoder.Text)
    encoder.integer(value)
  end
end

defimpl PGWire.Encoder, for: Float do
  def encode(value, opts) do
    encoder = Keyword.get(opts, :encoder, PGWire.Encoder.Text)
    encoder.float(value)
  end
end

defimpl PGWire.Encoder, for: List do
  def encode(value, opts) do
    encoder = Keyword.get(opts, :encoder, PGWire.Encoder.Text)
    encoder.list(value)
  end
end

defimpl PGWire.Encoder, for: BitString do
  def encode(value, opts) do
    encoder = Keyword.get(opts, :encoder, PGWire.Encoder.Text)
    encoder.string(value)
  end
end

defimpl PGWire.Encoder, for: Map do
  def encode(map, opts) do
    encoder = Keyword.get(opts, :encoder, PGWire.Encoder.Text)
    encoder.map(map)
  end
end

defimpl PGWire.Descriptor, for: Map do
  def encode_descriptor(map, _opts) do
    # Enum.map(map, &(&1 |> do_encode() |> elem(1)))
    Enum.map(map, fn {k, v} ->
      {_, desc} = do_encode(v)
      {k, desc}
    end)
  end

  defp do_encode(map) when is_map(map), do: PGWire.Encoder.Text.map(map)
  defp do_encode(list) when is_list(list), do: PGWire.Encoder.Text.list(list)
  defp do_encode(nil), do: PGWire.Encoder.encode("", [])
  defp do_encode(value), do: PGWire.Encoder.encode(value, [])
end
