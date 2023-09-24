# innosoftfusiongo-ical

API serving Innosoft Fusion Go schedule data as an iCalendar feed, plus a web UI.

- Web UI for setting options (requires a modern browser).
- Supports global notifications (as all-day events).
- Supports weekly recurrence (separating by activity/location/time, and weekdays where necessary).
- Generated iCalendar files are valid and work correctly with most clients (e.g., Thunderbird, Outlook, Google Calendar, ICSx5).
- Options for working around client quirks.
- Supports filtering activity/category/location.
- Caches data and handles errors properly.
- Automatic timezone detection.
- Optional JSON output for use in other applications.

The school ID can be found in `assets/config.json` in a branded `com.innosoftfusiongo.*` APK.

Google Calendar is not recommended if your schedule changes frequently or often has last-minute cancellations since it generally updates at most around once a day and cannot be force-refreshed.

I mostly made this for myself, but I've published it in case anyone else finds it useful. I host an instance limited to serving the Queen's University ARC schedule [here](https://ifgical.api.pgaskin.net/110).

Example JQ filter for the JSON output:

```
curl -s 'https://ifgical.api.pgaskin.net/110.json?fake_cancelled=1&no_notifications=1' | jq -r '
  .schedule | map(
    . as $activity
    | $activity.instances
    | map(select(.isExclusion == false))
    | map({location: $activity.location, startTime: $activity.startTime} * $activity.base * .))
  | [.[][]]
  | sort_by(._start_at)
  | map("\(.date_weekday[:3]) \(.date)   \(.startTime) - \(.endTime)   \((.activity+(" "*30))[:30]) \((.location+(" "*30))[:30]) \(.description[:60]+(if ((.description | length) > 60) then "..." else "" end))")
  | .[]'
```
