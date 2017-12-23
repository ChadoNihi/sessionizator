defmodule Sessionization.Structures.Event do
  @derive [Poison.Encoder]
  defstruct [:timestamp, :event_type, :user_id, :content_id]
end
