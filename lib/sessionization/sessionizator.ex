defmodule Sessionization.Sessionizator do
  @moduledoc """
    Assumptions & conventions:
    - valid JSON
    - "all the events always come on time and with monotonically increasing timestamp"
    - "Sessions can have multiple advertisements (ad_start, ad_end -pairs), multiple pauses (pause, play -pairs) and single track (start,heartbeats,end)"
    - no parallel sessions per user
    - the user can shut down "the session in any moment and then you cannot expect the end_ events to arrive ever"
    - sessions w/ paused tracks expire on 60 sec too
    - `ad_count` counts ads with no "ad_end" or "ad_start" events too
    - `session_start` correspond the "timestamp" of the first event received for a given "user_id"-"content_id" pair
    - the "~ten" in "Every ~ten seconds of played track there will be heartbeat event" is assumed as exactly 10

    TODOs:
    - use @spec
    - tests
    - event type strings to atoms
    - better error handling
    - update to Elixir 1.6 and use its formatter
  """

  @doc """
  Hello world.

  ## Examples

      iex> Sessionization.hello
      :world

  """
  alias Sessionization.Structures.Event

  @session_table :ses_table
  @timeout_sec 60
  @track_heartbeat_sec 10

  # def main([]), do: main([nil])
  def main(_args) do
    :ets.new(@session_table, [:named_table])
    # # optimization: compile the pattern we use to split the data
    # line_break = :binary.compile_pattern("\n")

    if true do
      File.stream!("/home/electrofish/Projects/sessionizator/data/dataset-big.json", read_ahead: 1_000)
    else
      IO.stream(:stdio, :line)
    end
    |> Stream.map(&Poison.decode!(&1, as: %Event{}))
    |> Stream.each(&process_event/1)
    |> Stream.run
  end

  defp new_session?(%Event
    {
      timestamp: tm,
      event_type: ev_type,
      content_id: cont_id
    },
    {_stored_u_id, stored_cont_id, %{last_tm: last_tm}}
  )
  do
    stored_cont_id != cont_id or ev_type == "stream_start" or tm - last_tm >= @timeout_sec
  end

  defp process_event(ev = %Event
    {
      timestamp: tm,
      event_type: ev_type,
      user_id: u_id,
      content_id: cont_id
    }
  )
  do
    with [
        record = {_u_id, _stored_cont_id, ses_data}
      ] <- :ets.lookup(@session_table, u_id)
    do
      cond do
        new_session?(ev, record) ->
          # output the last session
          record_2_json(record)
          |> IO.puts

          if ev_type == "stream_end" do
            # output a new session on the immediate "stream_end" event
            record_2_json({u_id, cont_id, %{ev_count: 1, last_tm: tm}})
            |> IO.puts
          else
            record_event(ev, ses_data)
          end

        :otherwise ->
          record_event(ev, ses_data)
      end

    else
      _ ->
        if ev_type == "stream_end" do
          # output a new session on the immediate "stream_end" event
          record_2_json({u_id, cont_id, %{ev_count: 1, last_tm: tm}})
          |> IO.puts
        else
          record_event(ev, %{})
        end
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
      timestamp: tm,
      event_type: ev_type,
      user_id: u_id,
      content_id: cont_id
    },
    ses_data
  )
  do
    new_ses_data = case ev_type do
      "stream_start" ->
        %{
          fst_tm: tm,
          track_playtm: 0,
          ad_count: 0
        }

      "ad_start" ->
        %{ ses_data |
          ad_count: Map.get(ses_data, :ad_count, 0) + 1
        }

      "track_heartbeat" ->
        Map.update(ses_data, :track_playtm, @track_heartbeat_sec, &(&1 + @track_heartbeat_sec))

      _ ->
        ses_data
    end

    :ets.insert(@session_table,
      {
        u_id,
        cont_id,
        Map.update(new_ses_data, :ev_count, 1, &(&1 + 1))
        |> Map.put(:last_tm, tm)
      }
    )
  end
end
