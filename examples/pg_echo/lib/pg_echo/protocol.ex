defmodule PGEcho.Protocol do
  use PGWire.Protocol

  alias PGWire.{Query, Protocol}
  alias PGWire.Authentication, as: A

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_authentication(%A{user: user, password: pass}, state) do
    if user == "hydra" and pass == "pass",
      do: {:ok, [], state},
      else: {:error, :not_authenticated, state}
  end

  @impl true
  def handle_query(%Query{statement: statement} = q, state) do
    msgs = encode_and_complete(q, [%{"echo" => statement}])

    {:ok, msgs, state}
  end

  defp encode_and_complete(query, rows) do
    [
      Protocol.encode_descriptor(rows),
      Protocol.encode_data(rows),
      Protocol.complete(query, length(rows)),
      Protocol.ready()
    ]
  end
end
