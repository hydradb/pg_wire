defmodule PGWire.Protocol do
  @type t :: module
  @callback init(state :: term) :: {:ok, term()} | {:error, term()}
  @callback handle_authenticate(any(), any()) :: {:ok, term()} | {:error, term(), term()}
  @callback handle_prepare(any(), any()) :: {:ok, term()} | {:error, term(), term()}
  @callback handle_query(args :: term, state :: term) ::
              {:ok, term(), term()} | {:error, term(), term()}
end
