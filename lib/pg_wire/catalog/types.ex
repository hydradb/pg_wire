defmodule PGWire.Catalog.Types do
  @external_resource pg_type_path = Path.join(__DIR__, "pg_type.json")

  pg_types =
    for line <- File.stream!(pg_type_path) do
      line
      |> Jason.decode!()
      |> Map.new(fn
        {"typname" = k, v} -> {String.to_atom(k), String.to_atom(v)}
        {k, v} -> {String.to_atom(k), v}
      end)
    end

  for type = %{typname: typname, oid: oid} <- pg_types do
    def type_info(unquote(typname)), do: unquote(Macro.escape(type))
    def oid_to_info(unquote(oid)), do: unquote(Macro.escape(type))
  end
end
