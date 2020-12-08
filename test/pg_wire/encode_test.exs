defmodule PGWire.EncodeTest do
  use ExUnit.Case

  describe "encode" do
    test "string/1 encodes variable size binaries" do
      bin = "test"
      assert {^bin, {_oid, typelen}} = PGWire.Encode.string(bin)
      assert -1 == typelen
    end

    test "atom/1 encodes as string/1" do
      a = :test
      assert PGWire.Encode.string(Atom.to_string(a)) == PGWire.Encode.atom(a)
    end

    test "integer/1 encode" do
      assert {bin, {_, typelen}} = PGWire.Encode.integer(1)
      assert bin == <<0, 0, 0, 1>>
      assert byte_size(bin) == typelen
    end

    test "float/1 encode" do
      assert {f_bin, {_, typelen}} = PGWire.Encode.float(1.0)
      assert f_bin == <<1.0::float>>
      assert byte_size(f_bin) == typelen
    end

    test "map/1 encode as json_t" do
      m = %{"a" => "b"}
      assert {json, {_, -1}} = PGWire.Encode.map(m)
      assert Jason.encode!(m) == json
    end

    test "list/1 encode as json_t" do
      l = [1, 2, 3]

      assert {json, {_, -1}} = PGWire.Encode.list(l)
      assert Jason.encode!(l) == json
    end
  end
end
