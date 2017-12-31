defmodule Sessionization.FastSessionizator do
  @moduledoc """
    Assumptions & conventions:
    - valid JSON
    - "all the events always come on time and with monotonically increasing timestamp"
    - "Sessions can have multiple advertisements (ad_start, ad_end -pairs), multiple pauses (pause, play -pairs) and single track (start,heartbeats,end)"
    - no parallel sessions per user: a new track means a new session for a given user
    - the user can shut down "the session in any moment and then you cannot expect the end_ events to arrive ever"
    - sessions w/ paused tracks expire on 60 sec too
    - `ad_count` counts ads with no "ad_end" or "ad_start" events too
    - `session_start` corresponds to the "timestamp" of the first event of a new session
    - the "~ten" in "Every ~ten seconds of played track there will be heartbeat event" is counted as exactly 10

    TODOs:
    - parallelize w/ GenServer or GenStage (Flow)
    - optimize IO (some ideas can be found here - https://elixirforum.com/t/help-with-performance-file-io/802)
    - use @spec
    - tests
    - event type strings to atoms
    - better error handling
    - update to Elixir 1.6 and use its formatter
    - change @track_heartbt_sec to the actual avg "track_heartbeat" interval
  """

  @doc """
  The sessionizator, processes events and outputs sessions.

  ## Examples

      iex> Sessionization.Sessionizator.main(["-f", "./sample_data/dataset_tiny.json"])
      :ok

  """
  alias Sessionization.Structures.Event

  @possible_events_after_track_ended [
    "ad_start", "ad_end", "stream_end"
  ]
  @session_table :ses_table
  @timeout_sec 60
  @track_heartbt_event_names ["track_hearbeat", "track_heartbeat"]
  @track_heartbt_sec 10

  def main(args) do
    path =
      case OptionParser.parse(args, switches: [file: :string], aliases: [f: :file]) do
        {[file: path], _, _} ->
          path
        {[], [], []} ->
          false
        _ ->
          IO.puts """
          Usage:\n
           <the_script_name> --file, -f\n\tfollowed by a path to a JSON-file with events\n
           <the_script_name>\n\tto feed events line by line
          """
          # call System.halt(0) ONLY if your app is a script
          # (see https://stackoverflow.com/a/28997461/4579279)
          System.halt(0)
      end

    :ets.new(@session_table,
      [
        :set, :named_table,
        # potential concurrency gains at some memory expenses (docs http://erlang.org/doc/man/ets.html#new-2)
        {:write_concurrency,true}, {:read_concurrency,true}
      ]
    )

    if path do
      File.stream!(path, [:raw, read_ahead: :math.pow(2, 22) |> round]) # read N bytes ahead
    else
      IO.stream(:stdio, :line)
    end
    |> Stream.each(fn(line) ->
      process_event(Poison.decode!(line, as: %Event{}))
    end)
    |> Stream.run

    process_timeouted_records()
    |> Stream.each(&(record_2_json(&1) |> IO.puts))
    |> Stream.run
  end


  defp new_session?(%Event
    {
      timestamp: tm,
      event_type: ev_type,
      content_id: cont_id
    },
    {_stored_u_id, stored_cont_id, ses_data = %{last_tm: last_tm}}
  )
  do
    stored_cont_id !== cont_id
    or ev_type === "stream_start"
    or tm - last_tm >= @timeout_sec
    or (
      (Map.get(ses_data, :track_ended?, false)
      and ev_type not in @possible_events_after_track_ended)
    )
  end


  def on_new_session(ev = %Event
    {
      timestamp: tm,
      event_type: ev_type,
      user_id: u_id,
      content_id: cont_id
    },
    record = {_stored_u_id, _stored_cont_id, ses_data}
  )
  do
    unless Enum.empty?(ses_data) do
      # i.e. we have the previous session to output
      record_2_json(record)
      |> IO.puts
    end

    if ev_type === "stream_end" do
      # immediately output the session that's got only the "stream_end" event
      record_2_json({u_id, cont_id, %{ev_count: 1, last_tm: tm}})
      |> IO.puts
    else
      record_event(ev, ses_data)
    end
  end


  def on_same_session(ev = %Event
    {
      timestamp: tm,
      event_type: ev_type,
      user_id: u_id
    },
    record = {_stored_u_id, _stored_cont_id, ses_data}
  )
  do
    if ev_type === "stream_end" do
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


  defp process_event(ev = %Event{user_id: u_id}) do
    with [record] <- :ets.lookup(@session_table, u_id)
    do
      cond do
        new_session?(ev, record) ->
          on_new_session(ev, record)

        :otherwise ->
          on_same_session(ev, record)
      end

    else
      _ ->
        on_new_session(ev, {nil, nil, %{}})
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
      ev_type when ev_type in @track_heartbt_event_names ->
        Map.put(ses_data, :tm_4_tract_last_heartbt_or_start, tm)
        |> Map.update(:track_playtm, @track_heartbt_sec, &(&1 + @track_heartbt_sec))

      "pause" ->
        sec_since_track_start_or_last_heartbt =
          tm - Map.get(ses_data, :tm_4_tract_last_heartbt_or_start, tm)
        Map.update(ses_data, :track_playtm, sec_since_track_start_or_last_heartbt, &(&1 + sec_since_track_start_or_last_heartbt))

      "ad_start" ->
        Map.put(ses_data, :ad_started?, true)
        |> Map.update(:ad_count, 1, &(&1 + 1))

      "ad_end" ->
        ad_started? = Map.pop(ses_data, :ad_started?, false)
        unless ad_started? do
          Map.update(ses_data, :ad_count, 1, &(&1 + 1))
        else
          ses_data
        end

      "stream_start" ->
        %{
          fst_tm: tm,
          track_playtm: 0,
          ad_count: 0
        }

      "track_start" ->
        Map.put(ses_data, :track_playtm, 0)
        |> Map.put(:tm_4_tract_last_heartbt_or_start, tm)

      "track_end" ->
        Map.put(ses_data, :track_ended?, true)

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


  defp process_timeouted_records() do
    match_spec = [{
      {:_, :_, %{last_tm: :"$1"}},
      [],
      [:"$_"]
    }]
    # stream timeouted sessions from ETS
    Stream.resource(
      fn -> :ets.select(@session_table, match_spec, 1_200) end,
      fn
        {records, continuation} ->
          {[records], :ets.select(continuation)}
        :"$end_of_table" ->
          {:halt, nil}
      end,
      fn _ -> :ok end
    )
    # flatten
    |> Stream.concat
  end
end
