defmodule PGWire.EncodeTest do
  use ExUnit.Case

  describe "encode" do
    test "string/1 encodes variable size binaries" do
      bin = "test"
      assert {^bin, %{typlen: typlen}} = PGWire.Encode.string(bin)
      assert -1 == typlen
    end

    test "atom/1 encodes as string/1" do
      a = :test
      assert PGWire.Encode.string(Atom.to_string(a)) == PGWire.Encode.atom(a)
    end

    test "integer/1 encode" do
      assert {bin, _} = PGWire.Encode.integer(1)
      assert bin == <<"1"::binary>>
    end

    test "float/1 encode" do
      assert {f_bin, _} = PGWire.Encode.float(1.0)
      assert f_bin == <<Float.to_string(1.0)::binary>>
    end

    test "map/1 encode as json_t" do
      m = %{"a" => "b"}
      assert {json, _} = PGWire.Encode.map(m)
      assert Jason.encode!(m) == json
    end

    test "list/1 encode as json_t" do
      l = [1, 2, 3]

      assert {json, _} = PGWire.Encode.list(l)
      assert Jason.encode!(l) == json
    end
  end
end
