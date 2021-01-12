# PGWire

The PostgreSQL backend protocol for Elixir. Allows the creation of Postgres compatible TCP servers.

## Usage

1. Implement the protocol behaviour

```elixir
defmodule PGEcho.Protocol do
  use PGWire.Protocol

  alias PGWire.{Query, Protocol}
  alias PGWire.Authentication, as: A

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_authentication(%A{user: user, password: pass} = a, state) do
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
```

2. Start the `PGWire` tcp server:

```elixir
iex> PGWire.start_link(PGEcho.Protocol, port: 5432)
```

3. Start a client and send a query;

```bash
➜ psql -U hydra -h localhost -c 'SELECT * FROM mytable'

         echo
-----------------------
 SELECT * FROM mytable
(1 row)
```

## Features
- [ ] Emulation of pg_catalog
- [ x ] Simple query
- [ ] Extended query
- [ x ] Notify / Listen
- [ ] Copy

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `pg_wire` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:pg_wire, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/pg_wire](https://hexdocs.pm/pg_wire).
