defmodule PGWire.ProtocolTest do
  use ExUnit.Case

  require Postgrex.Messages
  require PGWire.Messages

  alias PGWire.{Messages, Protocol}

  describe "ssl_request" do
    test "in :connected state stays in the state" do
      ssl_req = Messages.msg_ssl_request()

      assert {:keep, <<?N>>, _} = Protocol.handle_message(ssl_req, :connected, %{})
    end

    test "in other state returns error" do
      ssl_req = Messages.msg_ssl_request()

      assert {:error, _, _} = Protocol.handle_message(ssl_req, :other_state, %{})
    end
  end

  describe "startup" do
    setup do
      params = [user: "hydra", database: "hydra"]

      msg = Messages.msg_startup(params: params)
      {:ok, msg: msg}
    end

    test "in :connected state moves to next state", %{msg: msg} do
      assert {:next, _, _} = Protocol.handle_message(msg, :connected, %{session_params: %{}})
    end

    test "in :connected state returns authentication request", %{msg: msg} do
      auth_type = Messages.auth_type(:cleartext)

      auth_msg =
        [type: auth_type]
        |> Messages.msg_auth()
        |> Messages.encode_msg()

      assert {:next, [^auth_msg], _} =
               Protocol.handle_message(msg, :connected, %{session_params: %{}})
    end
  end

  describe "authentication" do
    setup do
      allow = Messages.msg_password(pass: "pg")
      deny = Messages.msg_password(pass: "p")

      {:ok, allow: allow, deny: deny, state: %{mod: T, state: %{}, session_params: %{}}}
    end

    test "with correct password moves to next state", %{allow: allow, state: state} do
      assert {:next, _, _} = Protocol.handle_message(allow, :unauthenticated, state)
    end

    test "with correct password sends auth ok & query ready messages to next state", %{
      allow: allow,
      state: state
    } do
      ok =
        [type: Messages.auth_type(:ok)]
        |> Messages.msg_auth()
        |> Messages.encode_msg()

      ready =
        [status: ?I]
        |> Messages.msg_ready()
        |> Messages.encode_msg()

      assert {:next, [^ok, ^ready], _} = Protocol.handle_message(allow, :unauthenticated, state)
    end

    test "with incorrect password returns error", %{deny: deny, state: state} do
      assert {:error, _, _} = Protocol.handle_message(deny, :unauthenticated, state)
    end
  end

  describe "termination" do
    test "sends no msg and moves to issues a stop" do
      msg = Messages.msg_terminate()
      assert {:disconnect, :normal, [], _} = Protocol.handle_message(msg, :ok, %{})
    end
  end
end

defmodule T do
  use PGWire.Handler

  def init(_) do
    {:ok, :ok}
  end

  def handle_authentication(%PGWire.Authentication{password: pass}, state) do
    if pass == "pg" do
      {:ok, [], state}
    else
      {:error, :not_authenticated, state}
    end
  end

  def handle_query(%PGWire.Query{} = _query, _state) do
    :ok
  end
end
