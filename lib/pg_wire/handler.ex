defmodule PGWire.Handler do
  @doc """
  Initialization of the handler. The handler should return any state
  it wishes to keep during the lifetime of the `connection`.
  """
  @callback init(args :: term) ::
              {:ok, state}
              | :ignore
              | {:stop, reason :: any}
            when state: any

  @doc """
  The `handle_authentication` callback is responsible for determining if the requests is authenticated or not.

  This callback is part of the start-up phase of the Postgres protocol.
  After the first part initialization, the postgres client will send an Authentication request.

  """
  @callback handle_authentication(auth :: any(), state :: term) ::
              {:ok, [message], new_state}
              | {:error, reason, new_state}
              | {status, new_state}
              | {:disconnect | :error, reason, [message], new_state}
            when new_state: term, reason: term, message: term, status: term

  @doc """

  This callback is called when the client has initiated a simple query.

  In the general case the response should contain the following messages:

  1. RowDescription
  1. DataRow | EmptyQueryResponse
  1. CommandComplete
  1. ReadForQuery

  For each row a `DataRow` message is transmitted
  """
  @callback handle_query(query :: any(), state :: term) ::
              {:ok, [message], new_state}
              | {status, new_state}
              | {:disconnect | :error, reason, [message], new_state}
            when new_state: term, reason: term, message: term, status: term

  defmacro __using__(_opts \\ []) do
    quote location: :keep do
      @behaviour PGWire.Handler
      import PGWire.Protocol,
        only: [
          encode_data: 1,
          encode_descriptor: 1,
          complete: 2,
          ready: 0
        ]
    end
  end
end
