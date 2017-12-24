defmodule SessionizationTest do
  use ExUnit.Case
  doctest Sessionization

  test "greets the world" do
    assert Sessionization.Sessionizator.main() == :ok
  end
end
