defmodule PGWire.ConnectionTest do
  use ExUnit.Case
  require Postgrex.Messages
  require PGWire.Messages

  alias PGWire.Connection

  describe "connected" do
    test "with ssl_message stays in `connected` state" do
      tcp_msg = tcp_message(Postgrex.Messages.msg_ssl_request())
      state = make_state()

      assert {:next_state, next_state, _, _} = Connection.connected(:info, tcp_msg, state)
      assert next_state == :connected
    end

    test "responds to ssl_message with a ?N" do
      tcp_msg = tcp_message(Postgrex.Messages.msg_ssl_request())
      state = make_state()

      assert {:next_state, next_state, _, _} = Connection.connected(:info, tcp_msg, state)
      assert assert_receive <<?N>>
    end

    test "with startup goes to `unauthenticated` state" do
      params = [user: "hydra", database: "hydra"]
      state = make_state()

      tcp_msg =
        [params: params]
        |> Postgrex.Messages.msg_startup()
        |> tcp_message()

      assert {:next_state, next_state, _, _} = Connection.connected(:info, tcp_msg, state)
      assert next_state == :unauthenticated
    end

    test "responds to startup msg with password challange" do
      params = [user: "hydra", database: "hydradb"]
      state = make_state()

      tcp_msg =
        [params: params]
        |> Postgrex.Messages.msg_startup()
        |> tcp_message()

      _ = Connection.connected(:info, tcp_msg, state)

      msg =
        [type: PGWire.Messages.auth_type(:cleartext)]
        |> PGWire.Messages.msg_auth()
        |> PGWire.Messages.encode_msg()

      assert_receive ^msg
    end
  end

  defp tcp_message(msg) do
    msg =
      msg
      |> Postgrex.Messages.encode_msg()
      |> :erlang.iolist_to_binary()

    tcp_msg = {:tcp, make_ref(), msg}
  end

  defp make_state do
    %Connection{transport: Kernel, socket: self()}
  end
end
