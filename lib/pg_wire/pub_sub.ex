defmodule PGWire.PubSub do
  @type t :: atom()
  @type message :: term()
  @type topic :: binary()

  @spec listen(t, topic, Keyword.t()) :: :ok | {:error, term()}
  def listen(pubsub, topic, opts \\ [])
      when is_atom(pubsub) and is_binary(topic) do
    case Registry.register(pubsub, topic, opts) do
      {:ok, _} -> :ok
      {:error, _reason} = err -> err
    end
  end

  @spec unlisten(t, topic) :: :ok
  def unlisten(pubsub, topic) do
    Registry.unregister(pubsub, topic)
  end

  @spec notify(t(), topic(), message()) :: :ok | {:error, term()}
  def notify(pubsub, topic, message) do
    Registry.dispatch(pubsub, topic, {__MODULE__, :dispatch, [{topic, message}]})
  end

  @spec dispatch(list(), term()) :: :ok
  def dispatch(entries, message) do
    for {pid, _} <- entries, do: send(pid, {:"$notify", message})
    :ok
  end
end
