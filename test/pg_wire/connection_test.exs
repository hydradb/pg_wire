defmodule PGWire.ConnectionTest do
  use ExUnit.Case
  require Postgrex.Messages
  require PGWire.Messages

  alias PGWire.Connection

  describe "state `connected`" do
    setup do
      state = make_state()

      {:ok, state: state}
    end

    test "with ssl_message stays in `connected` state", %{state: state} do
      tcp_msg = tcp_message(Postgrex.Messages.msg_ssl_request())

      assert {:next_state, next_state, _, _} = Connection.connected(:info, tcp_msg, state)
      assert next_state == :connected
    end

    test "responds to ssl_message with a ?N", %{state: state} do
      tcp_msg = tcp_message(Postgrex.Messages.msg_ssl_request())

      assert {:next_state, next_state, _, _} = Connection.connected(:info, tcp_msg, state)
      assert assert_receive <<?N>>
    end

    test "with startup goes to `unauthenticated` state", %{state: state} do
      params = [user: "hydra", database: "hydra"]

      tcp_msg =
        [params: params]
        |> Postgrex.Messages.msg_startup()
        |> tcp_message()

      assert {:next_state, next_state, _, _} = Connection.connected(:info, tcp_msg, state)
      assert next_state == :unauthenticated
    end

    test "responds to startup msg with password challange", %{state: state} do
      params = [user: "hydra", database: "hydradb"]

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

    test "responds to startup sets `session_params` on the state`", %{state: state} do
      params = [user: "hydra", database: "hydradb"]

      tcp_msg =
        [params: params]
        |> Postgrex.Messages.msg_startup()
        |> tcp_message()

      assert {:next_state, _, %Connection{session_params: p}, _} =
               Connection.connected(:info, tcp_msg, state)

      assert Map.get(p, :user) == "hydra"
      assert Map.get(p, :database) == "hydradb"
    end

    test "moves to `disconnect` state for any errors", %{state: state} do
      tcp_msg = {:tcp, nil, <<?E>>}

      assert {:next_state, :disconnect, _, _} = Connection.connected(:info, tcp_msg, state)
    end

    test "tcp_error -> {:stop, reason} ", %{state: state} do
      reason = :error
      tcp_msg = {:tcp_error, nil, reason}

      assert {:stop, ^reason} = Connection.connected(:info, tcp_msg, state)
    end
  end

  describe "state `unathenticated`" do
    setup do
      state = make_state(OkProtocol)

      tcp_msg =
        [pass: "pass"]
        |> Postgrex.Messages.msg_password()
        |> tcp_message()

      {:ok, state: state, tcp_msg: tcp_msg}
    end

    test "password message ok -> `idle` state", %{state: state, tcp_msg: tcp_msg} do
      assert {:next_state, :idle, _, _} = Connection.unauthenticated(:info, tcp_msg, state)
    end

    test "password message ok sends ok & ready", %{state: state, tcp_msg: tcp_msg} do
      ok =
        [type: PGWire.Messages.auth_type(:ok)]
        |> PGWire.Messages.msg_auth()
        |> PGWire.Messages.encode_msg()

      ready =
        [status: ?I]
        |> PGWire.Messages.msg_ready()
        |> PGWire.Messages.encode_msg()

      _ = Connection.unauthenticated(:info, tcp_msg, state)

      assert_receive ^ok
      assert_receive ^ready
    end

    test "password message error -> `diconnect` state", %{state: state} do
      assert {:next_state, :disconnect, _, _} =
               Connection.unauthenticated(:info, {:tcp, nil, <<?E>>}, state)
    end
  end

  describe "state `idle`" do
    setup do
      state = make_state()
      pid = start_supervised(PGWire.Connection.Supervisor)

      {:ok, state: state}
    end

    test "unknown message sends error but stays in :idle", %{state: state} do
      assert {:keep_state, new_state, []} = Connection.idle(:info, {:tcp, nil, <<?E>>}, state)
      assert_receive <<?E>>
    end

    test "starts a portal for a `simple query`", %{state: state} do
      q = Postgrex.Messages.msg_query(statement: "SELECT * FROM posts")
      msg = tcp_message(q)

      assert {:keep_state, %Connection{portals: portals}, []} = Connection.idle(:info, msg, state)
      refute Enum.empty?(portals)
      assert portals |> Map.values() |> List.first() == q
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
    %Connection{transport: Kernel, socket: self(), portals: %{}, session_params: %{}}
  end

  defp make_state(mod) do
    %Connection{
      transport: Kernel,
      socket: self(),
      portals: %{},
      session_params: %{},
      mod: mod,
      mod_state: %{}
    }
  end
end

defmodule OkProtocol do
  @behaviour PGWire.Protocol
  def init(_) do
    {:ok, %{}}
  end

  def handle_authenticate(_, state) do
    {:ok, state}
  end
end
