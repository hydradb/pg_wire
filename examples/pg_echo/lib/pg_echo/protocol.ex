defmodule PGEcho.Protocol do
  require Logger
  use PGWire.Protocol
  alias PGWire.{Authentication, Query, Protocol}

  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_authentication(%Authentication{} = a, state) do
    {:ok, [], state}
  end

  @impl true
  def handle_query(%Query{statement: statement} = q, state) do
    row =
      statement
      |> String.split(" ")
      |> Enum.with_index()
      |> Enum.map(fn {part, index} -> {"part#{index}", part} end)
      |> Enum.into(%{})

    msgs = encode_and_complete(q, [row])

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
