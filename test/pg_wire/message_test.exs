defmodule PGWire.MessageTest do
  use ExUnit.Case

  require Postgrex.Messages
  require PGWire.Messages

  import PGWire.BinaryUtils

  alias PGWire.Messages
  alias Postgrex.Messages, as: ClientMessages

  @startup_opts [user: "hydra", database: "hydra"]

  describe "decode" do
    test "SSL negotitation" do
      data = <<1234::int16, 5679::int16>>
      size = IO.iodata_length(data) + 4

      msg = <<size::int32>> <> data

      assert Messages.decode(msg) == Messages.msg_ssl_request()
    end

    test "startup msg" do
      msg = ClientMessages.msg_startup(params: @startup_opts) |> enc()

      assert Messages.decode(msg) ==
               Messages.msg_startup(params: %{user: "hydra", database: "hydra"})
    end

    test "startup msg with `options`" do
      msg = ClientMessages.msg_startup(params: @startup_opts ++ [options: "arg1 arg2"]) |> enc()

      assert Messages.decode(msg) ==
               Messages.msg_startup(
                 params: %{user: "hydra", database: "hydra", options: "arg1 arg2"}
               )
    end

    test "startup msg with unknowns are skipped" do
      msg = ClientMessages.msg_startup(params: @startup_opts ++ [unknown: "unknown"]) |> enc()

      assert Messages.decode(msg) ==
               Messages.msg_startup(params: %{user: "hydra", database: "hydra"})
    end

    test "password msg" do
      pass = "hydra" <> <<0>>
      msg =
        [pass: pass]
        |> ClientMessages.msg_password()
        |> enc()

      assert Messages.decode(msg) == Messages.msg_password(pass: "hydra")
    end

    test "query msg" do
      stmt = "SELECT * FROM posts;"

      msg =
        [statement: stmt]
        |> ClientMessages.msg_query()
        |> enc()

      assert Messages.decode(msg) == Messages.msg_query(statement: stmt)
    end

    test "terminate msg" do
      msg = ClientMessages.msg_terminate() |> enc()

      assert Messages.decode(msg) == Messages.msg_terminate()
    end
  end

  defp enc(msg) do
    msg
    |> Postgrex.Messages.encode_msg()
    |> :erlang.iolist_to_binary()
  end
end
