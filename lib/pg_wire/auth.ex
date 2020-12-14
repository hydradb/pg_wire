defmodule PGWire.Auth do
  @type t :: %__MODULE__{
          user: String.t(),
          database: String.t(),
          password: String.t(),
          kind: :cleartext | :md5
        }

  defstruct [
    :user,
    :database,
    :password,
    :kind
  ]

  def new(opts) do
    struct(__MODULE__, opts)
  end
end
