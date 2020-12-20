defmodule PGWire.Catalog.TypesTest do
  use ExUnit.Case

  alias PGWire.Catalog.Types

  test "type_info/1 returns type info for given type" do
    type = :bool
    assert %{typname: ^type} = Types.type_info(type)
  end

  test "has array types info for given type" do
    type = :_bool
    assert %{typname: ^type, typsend: typesend} = Types.type_info(type)
    assert typesend == "array_send"
  end

  test "oid_to_info/1 returns type info for given oid" do
    oid = 16
    type = :bool
    assert %{typname: ^type, oid: ^oid} = Types.oid_to_info(oid)
  end
end
