"""
Assumptions & conventions:
- valid JSON
- "all the events always come on time and with monotonically increasing timestamp"
- "Sessions can have multiple advertisements (ad_start, ad_end -pairs), multiple pauses (pause, play -pairs) and single track (start,heartbeats,end)"
- no parallel sessions per user
- the user can shut down "the session in any moment and then you cannot expect the end_ events to arrive ever"
- sessions w/ paused tracks expire on 60 sec too
- `ad_count` counts ads with no "ad_end" or "ad_start" events too
- `session_start` correspond the "timestamp" of the first event received for a given "user_id"-"content_id" pair

TODOs:
- use @spec
- tests
- event type strings to atoms
- better error handling
- update to Elixir 1.6 and use its formatter
"""


defmodule Sessionization.Sessionizator do
  @moduledoc """
  Documentation for Sessionization.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Sessionization.hello
      :world

  """
  alias Sessionization.Structures.Event

  @session_table :acc_session
  @timeout_sec 60

  # def main([]), do: main([nil])
  def main(_args) do
    :ets.new(@session_table, [:named_table])
    # # optimization: compile the pattern we use to split the data
    # line_break = :binary.compile_pattern("\n")

    if True do
      File.stream!("/home/electrofish/Projects/sessionizator/data/dataset-tiny.json", read_ahead: 1_000)
    else
      IO.stream(:stdio, :line)
    end
    |> Stream.map(&Poison.decode!(&1, as: %Event{}))
    |> Stream.each(&process_event/1)
    # |> wait_60sec
    # # |> Stream.into(File.stream!("new"))
    |> Stream.run

    # Application.app_dir :sessionization
    # |> Path.join("data/dataset-short.json")
    # |> File.stream!(read_ahead: 1_000)
    # |> Flow.from_enumerable()
    # |> Flow.flat_map(&(String.split(&1, line_break) |> Poison.decode!))
    # |> Flow.
  end

  defp new_session?(ev = %Event
    {
      user_id: u_id,
      content_id: cont_id,
      event_type: ev_type,
      timestamp: tm
    }
  ) do
    with
      [{_u_id, stored_cont_id, %{last_tm: last_tm}}] <- :ets.lookup(@session_table, u_id)
    do
      stored_cont_id != cont_id or ev_type == "stream_start" or tm - last_tm >= @timeout_sec
    else
      _no_res -> true
    end
  end

  defp process_event(ev = %Event
    {
      user_id: u_id,
      content_id: cont_id,
      event_type: ev_type
    }
  ) do
    with [record = {_u_id, stored_cont_id, _ses_data}] <- :ets.lookup(@session_table, u_id)
    do
      cond do
        new_session?(ev) ->
          record_2_json(record)
          |> IO.puts

          if ev_type == "stream_end", do: output(), else: record_event(ev, record)

        true ->
          record_event(ev, record)
      end

    else
      _no_res -> start_session(ev)
    end
  end

  defp record_2_json({u_id, cont_id, ses_data}) do
    ses_start = Map.get(ses_data, :fst_tm, nil)
    ses_end = Map.get(ses_data, :last_tm, nil)
    total_tm = try do
      ses_end - ses_start
    rescue
      ArithmeticError -> nil
    end

    Poison.encode!(
      %{
        user_id: u_id,
        content_id: cont_id,
        session_start: ses_start,
        session_end: ses_end,
        total_time: total_tm,
        track_playtime: Map.get(ses_data, :track_playtm, nil),
        event_count: Map.get(ses_data, :ev_count, nil),
        ad_count: Map.get(ses_data, :ad_count, nil)
      }
    )
  end

  defp record_event(%Event
    {
      event_type: ev_type,
      timestamp: tm
    },
    {u_id, stored_cont_id, ses_data}
  ) do

  end

  # defp process_event(%Event{event_type: "stream_end"}) do
  #
  # end
end
