defmodule PGWire.MessageTest do
  use ExUnit.Case

  import PGWire.Messages
  import PGWire.BinaryUtils

  alias PGWire.Messages

  @startup_opts [user: "hydra", database: "hydra"]

  describe "decode" do
    test "SSL negotitation" do
      data = <<1234::int16, 5679::int16>>
      size = IO.iodata_length(data) + 4

      msg = <<size::int32>> <> data

      assert Messages.decode(msg) == msg_ssl_request()
    end

    test "startup msg" do
      msg = msg_startup(params: @startup_opts) |> enc()

      assert Messages.decode(msg) ==
               msg_startup(params: %{user: "hydra", database: "hydra"})
    end

    test "startup msg with `options`" do
      msg = msg_startup(params: @startup_opts ++ [options: "arg1 arg2"]) |> enc()

      assert Messages.decode(msg) ==
               msg_startup(params: %{user: "hydra", database: "hydra", options: "arg1 arg2"})
    end

    test "startup msg with unknowns are skipped" do
      msg = msg_startup(params: @startup_opts ++ [unknown: "unknown"]) |> enc()

      assert Messages.decode(msg) ==
               msg_startup(params: %{user: "hydra", database: "hydra"})
    end

    test "password msg" do
      msg = msg_password(pass: "hydra")

      encoded =
        msg
        |> Postgrex.Messages.encode_msg()
        |> :erlang.iolist_to_binary()

      assert Messages.decode(encoded) == msg_password(pass: "hydra")
    end
  end

  defp enc(msg) do
    msg
    |> Postgrex.Messages.encode_msg()
    |> :erlang.iolist_to_binary()
  end

  # defp startup_msg(params \\ []) do
  #   params = [user: "hydra", database: "hydra"] ++ params

  #   params =
  #     Enum.reduce(params, [], fn {key, value}, acc ->
  #       [acc, to_string(key), 0, value, 0]
  #     end)

  #   vsn = <<3::int16, 0::int16>>
  #   data = :erlang.iolist_to_binary([vsn, params, 0])
  #   size = IO.iodata_length(data) + 4

  #   <<size::int32, data::binary>>
  # end
end
