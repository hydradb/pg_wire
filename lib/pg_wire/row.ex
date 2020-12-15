defmodule PGWire.RowDesc do
  @type t :: %__MODULE__{
          columns: list()
        }
  defstruct [:columns]
end

defmodule PGWire.RowData do
  @type t :: %__MODULE__{
    values: list()
  }

  defstruct [:values]
end


defmodule PGWire.Row do
  @type t :: %__MODULE__{
    desc: PGWire.RowDesc.t(),
    data: list(PGWire.RowData.t())
  }

  defstruct [:desc, :data]
end
