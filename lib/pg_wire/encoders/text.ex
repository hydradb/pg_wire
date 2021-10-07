defmodule PGWire.Encoder.Text do
  @behaviour PGWire.Encode

  alias PGWire.Catalog.Types

  @impl true
  def string(bin) when is_binary(bin), do: {<<bin::binary>>, Types.type_info(:text)}

  def atom(nil) do
    {<<"NULL"::binary>>, %{}}
  end

  @impl true
  def atom(true) do
    o = Types.type_info(:bool)
    {<<"true"::binary>>, o}
  end

  def atom(false) do
    o = Types.type_info(:bool)
    {<<"false"::binary>>, o}
  end

  def atom(atom) do
    atom
    |> to_string()
    |> string()
  end

  @impl true
  def integer(int) do
    o = Types.type_info(:int8)
    int = Integer.to_string(int)
    {<<int::binary>>, o}
  end

  @impl true
  def float(f) when is_float(f) do
    o = Types.type_info(:float8)

    f = Float.to_string(f)
    {<<f::binary>>, o}
  end

  @impl true
  def map(map) do
    o = Types.type_info(:json)
    json = Jason.encode!(map)

    {<<json::binary>>, o}
  end

  @impl true
  def list(list) do
    o = Types.type_info(:json)
    json = Jason.encode!(list)

    {<<json::binary>>, o}
  end
end
