defmodule PGWire.Types.OID do
  @type oid :: integer()
  @type typlen :: integer()
  @type t :: {oid(), typlen()}

  @spec boolean() :: t()
  def boolean, do: {16, 1}

  @spec int() :: t()
  def int, do: {20, 8}

  @spec float() :: t()
  def float, do: {701, 8}

  @spec string() :: t()
  def string, do: {25, -1}

  @spec json_t() :: t()
  def json_t, do: {114, -1}

  @spec json_b() :: t()
  def json_b, do: {3802, -1}
end
