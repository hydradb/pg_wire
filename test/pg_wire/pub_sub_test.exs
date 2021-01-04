defmodule PGWire.PubSubTest do
  use ExUnit.Case

  setup_all do
    start_supervised!({Registry, keys: :duplicate, name: PGWire})
    :ok
  end

  describe "listen/3 & unlisten/1" do
    test "returns ok" do
      assert :ok = PGWire.PubSub.listen(PGWire, "topic")
    end

    test "expects binary topic" do
      assert_raise FunctionClauseError, ~r/.*/, fn ->
        PGWire.PubSub.listen(PGWire, :topic)
      end
    end

    test "can always unlisten" do
      assert :ok = PGWire.PubSub.unlisten(PGWire, "topic")
    end
  end

  describe "notify/3" do
    test "broadcast to all listeners" do
      ch = "channel"
      msg = "message"
      assert :ok = PGWire.PubSub.listen(PGWire, ch)
      assert :ok = PGWire.PubSub.notify(PGWire, ch, msg)

      assert_receive {:"$notify", {^ch, ^msg}}
    end
  end
end
