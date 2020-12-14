defmodule PGEcho.Protocol do
  require Logger
  @behaviour PGWire.Protocol
  alias PGWire.{Auth, Query}

  def init(_opts) do
    {:ok, %{}}
  end

  def handle_authenticate(%Auth{} = a, state) do
    {:ok, state}
  end

  def handle_query(%Query{statement: statement} = q, state) do
    row =
      statement
      |> String.split(" ")
      |> Enum.with_index()
      |> Enum.map(fn {part, index} -> {"part#{index}", part} end)
      |> Enum.into(%{})

    {:ok, [row], state}
  end
end
