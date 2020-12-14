defmodule PGWire.RowDesc do
  @type t :: %__MODULE__{
          columns: list()
        }
  defstruct [:columns]
end
