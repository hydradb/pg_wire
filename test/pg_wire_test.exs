defmodule PGWireTest do
  use ExUnit.Case
  doctest PGWire

  test "greets the world" do
    assert PGWire.hello() == :world
  end
end
