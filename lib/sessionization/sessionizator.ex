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
    - change @track_heartbt_sec to the actual avg "track_heartbeat" interval
  """

  @doc """
  Hello world.

  ## Examples

      iex> Sessionization.main
      nil

  """
  alias Sessionization.Structures.Event

  @session_table :ses_table
  @timeout_sec 60
  @track_heartbt_event_names ["track_hearbeat", "track_heartbeat"]
  @track_heartbt_sec 10

  def main([]), do: main([nil])
  def main([path]) do
    :ets.new(@session_table, [:set, :named_table])
    # # optimization: compile the pattern we use to split the data
    # line_break = :binary.compile_pattern("\n")

    if path do
      File.stream!(path, read_ahead: 1_000)
    else
      IO.stream(:stdio, :line)
    end
    |> Stream.map(&Poison.decode!(&1, as: %Event{}))
    |> Stream.each(fn(ev = %Event{timestamp: tm}) ->
      # process_event() comes first:
      #   find_timeouted_records() assumes the event's user
      #   (in particular the user's potential expired session) has been dealt with
      process_event(ev)

      take_and_stream_timeouted_records(tm-60)
      |> Stream.each(&(record_2_json(&1) |> IO.puts))
      |> Stream.run
    end)
    |> Stream.run
  end
  def main(_), do: IO.puts "Give me a path to a JSON-file w/ events\nor feed me the events line by line."

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
        record = {_stored_u_id, _stored_cont_id, ses_data}
      ] <- :ets.lookup(@session_table, u_id)
    do
      cond do
        new_session?(ev, record) ->
          # output the previous session
          record_2_json(record)
          |> IO.puts

          if ev_type == "stream_end" do
            # immediately output the session that's got only the "stream_end" event
            record_2_json({u_id, cont_id, %{ev_count: 1, last_tm: tm}})
            |> IO.puts
          else
            record_event(ev, ses_data)
          end

        :otherwise ->
          if ev_type == "stream_end" do
            record_2_json(
              put_elem(record, 2,
                %{ses_data | ev_count: ses_data[:ev_count] + 1, last_tm: tm}
              )
            )
            |> IO.puts

            :ets.delete(@session_table, u_id)
          else
            record_event(ev, ses_data)
          end
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
        Map.update(ses_data, :ad_count, 1, &(&1 + 1))

      "track_start" ->
        Map.put_new(ses_data, :track_playtm, 0)
        |> Map.put(:tm_4_tract_last_heartbt_or_start, tm)

      ev_type when ev_type in @track_heartbt_event_names ->
        Map.put(ses_data, :tm_4_tract_last_heartbt_or_start, tm)
        |> Map.update(:track_playtm, @track_heartbt_sec, &(&1 + @track_heartbt_sec))

      "pause" ->
        sec_since_track_start_or_last_heartbt =
          tm - Map.get(ses_data, :tm_4_tract_last_heartbt_or_start, tm)
        Map.update(ses_data, :track_playtm, sec_since_track_start_or_last_heartbt, &(&1 + sec_since_track_start_or_last_heartbt))

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

  defp take_and_stream_timeouted_records(timeout_tm) do
    match_spec = [{
      {:_, :_, %{last_tm: :"$1"}},
      [{:"=<", :"$1", timeout_tm}],
      [:"$_"]
    }]
    Stream.resource(
      fn -> :ets.select(@session_table, match_spec, 1_000) end,
      fn
        {records, continuation} ->
          {[records], :ets.select(continuation)}
        :"$end_of_table" ->
          {:halt, nil}
      end,
      fn _ -> :ets.select_delete(@session_table, match_spec) end
    )
    |> Stream.concat
  end
end
