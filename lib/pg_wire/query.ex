defmodule PGWire.Query do
  @type t :: %__MODULE__{
          statement: String.t(),
          tag: reference()
        }

  defstruct [:statement, :tag]
end
