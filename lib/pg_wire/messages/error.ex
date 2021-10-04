defmodule PGWire.Messages.Error do
  require PGWire.Messages
  alias PGWire.{Messages, Error}

  @spec fatal(atom(), Keyword.t()) :: iolist()
  def fatal(error_name, opts \\ []) do
    error_name
    |> Error.fatal(opts)
    |> encode_error()
  end

  @spec error(atom(), Keyword.t()) :: iolist()
  def error(error_name, opts \\ []) do
    error_name
    |> Error.error(opts)
    |> encode_error()
  end

  @spec warning(atom(), Keyword.t()) :: iolist()
  def warning(error_name, opts \\ []) do
    error_name
    |> Error.warning(opts)
    |> encode_error()
  end

  defp encode_error(error) do
    error_fields = Error.to_map(error)

    [fields: error_fields]
    |> Messages.msg_error()
    |> Messages.encode_msg()
  end
end
