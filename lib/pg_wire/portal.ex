defmodule PGWire.Portal do
  use GenServer
  require PGWire.Messages
  require Logger

  alias PGWire.Messages

  @type mode :: :simple | :extended

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  @spec start_portal({term(), term()}, Keyword.t()) :: Supervisor.on_start_child()
  def start_portal({mod, mod_state}, opts \\ []) do
    child_spec = child_spec([{mod, mod_state}, opts])

    DynamicSupervisor.start_child(PGWire.PortalSupervisor, child_spec)
  end

  @spec start_link({term(), pid(), mode()}, Keyword.t()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @spec query(pid(), term(), term()) :: :ok
  def query(pid, query, callback) do
    GenServer.cast(pid, {:query, {self(), callback}, query})
  end

  @spec init(tuple()) :: {:ok, term()} | {:error, term()}
  def init({mod, state}) do
    {:ok, %{mod: mod, state: state}}
  end

  def handle_cast(
        {:query, {from, callback}, %PGWire.Query{statement: statement, tag: tag} = q},
        %{mod: mod, state: mod_state} = state
      ) do
    result =
      case mod.handle_query(q, mod_state) do
        {:ok, %PGWire.RowDesc{} = desc, new_mod_state} ->
          :ok = callback.(from, {:descriptor, tag, desc})
          {:noreply, %{state | state: new_mod_state}}

        {:ok, rows, new_mod_state} ->
          descriptor = row_description(rows)
          data = row_data(rows)
          complete = complete(statement, length(rows))

          :ok = callback.(from, {:complete, tag, [descriptor, data, complete]})

          {:noreply, %{state | state: new_mod_state}}

        {:error, reason} ->
          {:stop, reason}
      end

    Logger.info("#{inspect(result)}")
    result
  end

  defp row_data(rows) when is_list(rows) do
    for row <- rows, do: row_data(row)
  end

  defp row_data(row) when is_map(row) do
    values = PGWire.Encoder.encode(row, [])

    [values: values]
    |> Messages.msg_data_row()
    |> Messages.encode_msg()
  end

  defp row_description([row | _]) when is_map(row), do: row_description(row)

  defp row_description(row) when is_map(row) do
    desc = PGWire.Descriptor.encode_descriptor(row, [])

    [fields: desc]
    |> Messages.msg_row_desc()
    |> Messages.encode_msg()
  end

  defp complete(statement, affected) do
    [command | _] = String.split(statement, " ")
    tag = command <> " " <> to_string(affected)

    [tag: tag]
    |> Messages.msg_command_complete()
    |> Messages.encode_msg()
  end
end
