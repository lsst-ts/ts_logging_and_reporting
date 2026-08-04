# Capture comparison

This file contains the results of running `scripts/capture_endpoints.py`
on the service both before and after the caching refactor. This fetches
each endpoint for both a 1 and 7 day block, and compares the responses.
A verdict of `OK` indicates that the endpoint produces a `byte-identical`
response after the refactor, while any that do not have a section explaining
why underneath the table.

The actual generated json files are not included due to both size and
the fact they contain direct data from ConsDB/EFD. They can be generated
using the `scripts/capture_endpoints.py` script.

In summary of the inconsistencies: some are unavoidable noise, some are
harmless reorderings which are re-sorted on the frontend anyway, some are
actual bugfixes, and several warrant followup.

## Compare script output table

| File | Verdict | Differences |
|---|---|---|
| `almanac__1day.json` | OK |  |
| `almanac__7day.json` | OK |  |
| `block-details.json` | OK |  |
| `context-feed__1day.json` | DIFFERS | 3 |
| `context-feed__7day.json` | DIFFERS | 3 |
| `data-log__1day__LATISS.json` | OK |  |
| `data-log__1day__LSSTCam.json` | ORDER ONLY | 1 |
| `data-log__7day__LATISS.json` | OK |  |
| `data-log__7day__LSSTCam.json` | ORDER ONLY | 1 |
| `expected-exposures__1day.json` | DIFFERS (HTTP 500) | 1 |
| `expected-exposures__7day.json` | DIFFERS (HTTP 500) | 1 |
| `exposure-entries__1day__LATISS.json` | DIFFERS | 2 |
| `exposure-entries__1day__LSSTCam.json` | OK |  |
| `exposure-entries__7day__LATISS.json` | DIFFERS | 2 |
| `exposure-entries__7day__LSSTCam.json` | OK |  |
| `exposure-flags__1day__LATISS.json` | DIFFERS | 2 |
| `exposure-flags__1day__LSSTCam.json` | OK |  |
| `exposure-flags__7day__LATISS.json` | DIFFERS | 2 |
| `exposure-flags__7day__LSSTCam.json` | OK |  |
| `exposures__1day__LATISS.json` | DIFFERS (HTTP 502) | 12 |
| `exposures__1day__LSSTCam.json` | OK |  |
| `exposures__7day__LATISS.json` | DIFFERS (HTTP 502) | 12 |
| `exposures__7day__LSSTCam.json` | ORDER ONLY | 1 |
| `jira-tickets__1day__LATISS.json` | OK |  |
| `jira-tickets__1day__LSSTCam.json` | OK |  |
| `jira-tickets__7day__LATISS.json` | ORDER ONLY | 1 |
| `jira-tickets__7day__LSSTCam.json` | DIFFERS | 2 |
| `multi-night-visit-maps__1day__LATISS.json` | DIFFERS | 6 |
| `multi-night-visit-maps__1day__LSSTCam.json` | DIFFERS | 8 |
| `multi-night-visit-maps__7day__LATISS.json` | DIFFERS | 6 |
| `multi-night-visit-maps__7day__LSSTCam.json` | DIFFERS | 8 |
| `narrative-log__1day__LATISS.json` | OK |  |
| `narrative-log__1day__LSSTCam.json` | OK |  |
| `narrative-log__7day__LATISS.json` | OK |  |
| `narrative-log__7day__LSSTCam.json` | OK |  |
| `night-reports__1day.json` | OK |  |
| `night-reports__7day.json` | OK |  |
| `obs-status__1day.json` | OK |  |
| `obs-status__7day.json` | OK |  |
| `static-visit-map__1day__LATISS.json` | OK |  |
| `static-visit-map__1day__LSSTCam.json` | OK |  |
| `static-visit-map__7day__LATISS.json` | OK |  |
| `static-visit-map__7day__LSSTCam.json` | OK |  |

Note that the `before` capture was taken on 3 August and the `after` capture on
4 August, so a difference can also be upstream data changing between the two
runs; where that is the cause, the section says so.

## Findings

### `context-feed__1day.json`, `context-feed__7day.json`

The difference count is misleading: it is three lines describing thousands of
records, nearly all differing in a single field. Pairing on
`(time, name, script_salIndex)` — unique across every record on both sides —
almost every record pairs up, and the handful that do not are covered below.

Six causes. The first three appear in both spans, the last three only in the
7-day capture. Those last three share a single root, so that comes first.

**The dayobs boundary.** The cache is keyed per dayobs, and a request fetches
only the days it misses, one query per contiguous run of them. Where a run
begins therefore depends on what is already warm, and a run boundary can fall
at any noon inside the requested range. Each query is bounded by the dayobs it
is fetching, so it cannot see anything upstream publishes after that boundary —
and the EFD has records that belong to one dayobs but only become visible during
the next. Causes 4 to 6 are all instances of that.

**1. `"NaT"` became `null`.** The old path ran only `stringify_special_floats`,
which handles float NaN/Inf but not `pd.NaT`, so a null timestamp serialised as
the literal string `"NaT"`. `make_json_safe` now maps it to a real JSON `null`.
This accounts for the overwhelming majority of the differing fields, across all
five timestamp columns.

A fix, but a frontend-visible type change: a truthiness check saw a truthy
`"NaT"` before and gets `null` now. **This is the only change in the refactor
that requires a corresponding change in the frontend** — everything else in this
document is either invisible to it, filtered out client-side, or a reordering it
already re-sorts.

**2. Microsecond to nanosecond precision.** `...290646+00:00` became
`...290645999+00:00` on Task Change rows.

This is not a formatting change; the values have different provenance.
`ContextFeedService.collate_response` recomputes `timestampProcessEnd` for
"Task Change" rows by copying another record's `time`:

```python
# Recompute task-change rows timestampProcessEnd
task_changes = [i for i, r in enumerate(records) if r.get("finalStatus") == "Task Change"]
for position, index in enumerate(task_changes):
    next_position = position + 1
    if next_position < len(task_changes):
        records[index]["timestampProcessEnd"] = records[task_changes[next_position]]["time"]
    else:
        records[index]["timestampProcessEnd"] = records[-1]["time"]
```

`time` comes from the EFD `DatetimeIndex`, which is `datetime64[ns]`, so the
value assigned carries full nanosecond precision. The old value was computed
inside `rubin_nights` and passed through astropy's `Time(...).utc.datetime`,
which returns a Python `datetime` — microseconds, and **rounded**. The rounding
is the fingerprint: `290645999 ns` rounds to `290646 µs`, whereas pandas' own
`to_pydatetime()` would have truncated it to `290645`.

Every affected record is a Task Change row and every new value matches another
record's `time` verbatim. Each span leaves exactly one Task Change row
unchanged: the last, which takes the `else` branch and copies
`records[-1]["time"]`, a value that already matched.

**The frontend cannot see the difference.** `utils.js` parses these strings with
Luxon and works in milliseconds from there — `toMillis()` for the timeline,
`toFormat(...)` for display — and Luxon's resolution is the millisecond, so
everything below it is discarded on parse. The old microsecond value and the new
nanosecond one land on the same millisecond and render identically. This is
unrelated to the ns/µ obs-status issue usdf-dev recently suffered.

**3. Configuration records predating the window are dropped.** These arrive
unrequested: `get_consolidated_messages` deliberately reaches back for the last
known configuration via `select_top_n(num=1, time_cut=...)`, so the response
carries rows timestamped before the requested range. The old code forwarded
whatever came back; the new per-dayobs bucketing files them under their own
dayobs, which the service never requests, so they are dropped.

**Not a user-visible loss.** `ContextFeed.jsx` filters every row against
`selectedTimeRange`, which defaults to `fullTimeRange`, derived directly from the
dayobs in the URL. These records were fetched, serialised, sent and then
discarded client-side. They have never been displayed.

Nothing else reaches outside the requested range this way — this is the only
source in `get_consolidated_messages` that deliberately looks further back than
it was asked to.

**4. An image row inside the window is lost**, in the 7-day capture only. An
Image Acquired row reporting a time inside the requested range, so unlike the
configuration records above the frontend filter would have shown it.

The cause is a seam in `rubin_nights`. Image-acquisition rows are *selected* by
the EFD's publish timestamp, then *re-indexed* onto `timestampAcquisitionStart`:

```python
image_acquisition_mt = efd_client.select_time_series(topic, fields, t_start, t_end)
...
image_acquisition_mt.index = image_acquisition_mt["timestampAcquisitionStart"].copy()
```

Acquisition start precedes publication by the exposure plus readout, so a row
belonging to one dayobs can be published inside the next one's window. Only 3 of
the 11 sources `get_consolidated_messages` assembles do this — the other 8 keep
the EFD index — and nothing widens the query to compensate. The underlying values
are unix-TAI and *are* correctly converted to UTC, so the 37-second leap-second
offset is not the culprit; the publish lag is.

**5. A completed script reported as still running**, in the 7-day capture only:

```
finalStatus:          DONE                        -> RUNNING
timestampProcessEnd:  <a real timestamp>          -> 1969-12-31T23:59:51.999918
```

That date is not corruption. It is `Time(0, format="unix_tai").utc.datetime`,
which `rubin_nights` itself names `TIMESTAMP_ZERO` — the value an unset
timestamp field carries. The script simply has no recorded end.

`get_script_state` collapses every `ScriptQueue.logevent_script` message for a
script into one row:

```python
script_status = scripts.groupby("script_salIndex").agg({
    ...
    "finalScriptState": "max",
    "timestampProcessEnd": "max",
})
```

The aggregate is taken over the messages **in the query window**. This script
started before a noon boundary and finished after it, so the window that
produced its bucket closed with the script still mid-flight. The row is still
indexed on its start (`_find_best_script_time` prefers `timestampRunStart`), so
it is kept under the right dayobs — just truncated. Its start-side fields are
all correct, because `logevent_script` is a snapshot topic and every message
carries all five timestamps; only the terminal state needs a message that does
not exist yet.

**6. Whole-number exposure times rendered without a decimal**, in the 7-day
capture only, on Image Acquired rows:

```
config: 'exp 2.0 // dark 2.227060317993164 // open 2.0 '
     -> 'exp 2 // dark 2.227060317993164 // open 2 '
```

`rubin_nights` builds the string by interpolating the raw cell:

```python
def make_config_col_for_image(x: pd.Series) -> str:
    return f"exp {x.exposureTime} // dark {x.darkTime} // open {x.measuredShutterOpenTime} "
```

and the frame comes from `InfluxQueryClient._to_dataframe`, which does
`pd.DataFrame(series.get("values", []), columns=series["columns"])`. InfluxDB's
JSON encoder writes a float holding `2.0` as the JSON number `2`, so pandas
infers each column's dtype from the rows in *that one query*: all integral gives
`int64` and renders `2`, any fractional value gives `float64` and renders `2.0`.

Which nights share a query therefore decides the rendering. The `before` capture
queried the whole span at once, so a single night with fractional exposure times
forced `float64` on every row. The `after` capture splits at the run boundary,
and the trailing chunk happened to contain only whole-number exposures.

The payload carries its own proof that this is per-column and not a change to
the string builder: `darkTime` is fractional throughout, stays `float64` in both
chunks, and renders identically across the whole diff. Only `exp` and `open`
move. It also leaves the `after` payload internally inconsistent, with adjacent
nights rendering the same quantity differently in one response.

#### These predate the refactor

Causes 4, 5 and 6 all read as caching bugs and none of them is. Captured across
two separate before/after pairs, the same records behave like this:

| | `before` 1-day | `before` 7-day | `after` 1-day | `after` 7-day |
|---|---|---|---|---|
| script `finalStatus` | `RUNNING` | `DONE` | `RUNNING` | `RUNNING` |
| the lost image row | absent | present | absent | absent |

The old code produced the truncated answer whenever the requested range ended at
the boundary and the correct one whenever it did not. Both effects reproduce
identically in the earlier capture pair, so they are stable behaviour rather
than capture noise.

What the refactor changes is not the defect but its reach. A wide request used
to issue one wide query and incidentally see past the boundary; it now composes
its answer from per-dayobs entries, so it inherits whatever each entry's window
could see. It is also persisted: `RefreshWorker.refresh` finalises each dayobs
with a single-day run after rollover, and `_ttl` gives that write
`HISTORIC_TTL_REDIS`, so the truncated value is the one that sticks. Note that a
shorter TTL fixes nothing — the window is defined by the dayobs, not by
wall-clock, so re-fetching later re-issues the identical query.

Cause 6 is the same story by construction, since the rendering depends on which
nights share a query. Unlike 4 and 5 no capture demonstrates it directly, so it
is read from the code rather than shown.

#### Mitigation

`RubinNightsContextAdapter._fetch_run` now queries past the end of its run:

```python
RUN_END_MARGIN = dt.timedelta(hours=6)
...
t_end = Time(
    get_utc_datetime_from_dayobs_str(add_or_subtract_dayobs_days(run_end, 1))
    + RUN_END_MARGIN
)
```

Records are still bucketed by their own dayobs and `_collate_runs` still drops
any bucket the run does not own, so the extra rows cannot leak into a
neighbouring day's entry — the margin only lets a run see events that belong to
it but land late. That covers causes 4 and 5.

Only the trailing edge needs it. Image rows are selected on publish time and
re-indexed onto an earlier acquisition time, so their evidence can arrive after
the index but never before it; script rows read their start-side timestamps from
snapshots that any later message carries; and the remaining sources keep the EFD
publish index, so selection time and index are the same value.

Two caveats for review. The margin is a judgement call, not a bound — it has to
exceed the publish lag and the overrun of a script straddling noon, and no fixed
margin is sufficient for an arbitrarily long script. And it does not address
cause 6, which needs the upstream formatting fixed.

The `after` capture was taken before this landed, so the table above still shows
the unmitigated behaviour; re-running the capture is the check that it works.

### `data-log__1day__LSSTCam.json`, `data-log__7day__LSSTCam.json`

Identical records, different order — same multiset both sides. The new order is
deliberate.

`DataLogService.collate_response` now sorts:

```python
records = flatten_within_dayobs(data, "seq_num")
```

which walks the dayobs buckets in ascending order and sorts each night's records
by `seq_num`, giving `(day_obs, seq_num)`. Verified directly: both `after`
payloads are exactly in that order and neither `before` payload is.

Before the refactor there was no ordering to speak of. Neither the old query nor
the old Python path sorted, so `/data-log` returned whatever Postgres produced —
in the 7-day capture that means nights interleaved and sequence numbers out of
order within them. That it changed at all tracks the added join: `EFD_FIELDS` is
non-empty only for `lsstcam`, so only LSSTCam picks up the third join to the
transformed-EFD table, and only the LSSTCam files reordered.

The LATISS files are `OK` because Postgres happened to return those rows in
`(day_obs, seq_num)` order already — both `before` LATISS payloads are exactly
sorted, so the new explicit sort is a no-op for them. That is a coincidence of
the query plan rather than a guarantee, which is the argument for sorting
explicitly.

`/exposures` and `/data-log` read the same cache entry and now agree on
ordering.

### `expected-exposures__1day.json`, `expected-exposures__7day.json`

Both sides return **HTTP 500** — this endpoint fails in this environment before
and after. Only the message changed:

```
'Unable to locate credentials' -> 'Internal error in ExpectedExposuresService'
```

That is the deliberate change to stop leaking exception detail to clients. The
old response handed the caller a raw exception string; the new one is generic and
the detail goes to `logger.exception` instead.

The underlying failure has nothing to do with the refactor — the simulation
archive needs credentials this environment does not have.

**This endpoint's payload is therefore not covered by this comparison.** Both
captures record a failure, so the parity claim for `/expected-exposures` rests on
its unit tests rather than on this evidence.

**However, this endpoint shows the new error handling system.** Instead of
potentially leaking exception information, the frontend is only passed a
generic "Internal error in ..." message, while the actual error (including
traceback) is logged.

### `exposure-entries__{1day,7day}__LATISS.json`, `exposure-flags__{1day,7day}__LATISS.json`

**The instrument filter did not work before and does now.** The records that
"disappear" were never LATISS data.

The cause is a parameter-name mismatch. `ExposurelogAdapter.get_messages` sent
`instrument=<name>`, but the Exposure Log `/messages` endpoint defines no such
parameter — its filter is `instruments`, an array ("Names of instruments (e.g.
LSSTCam). Repeat the parameter for each value.", per the service's OpenAPI
schema). Unrecognised query parameters are ignored rather than rejected, so the
request succeeded and returned every instrument's messages.

The `obs_id` prefix names the telescope — `MC_O_` for MainTel/LSSTCam, `AT_O_`
for AuxTel/LATISS — and **every** record removed from the LATISS responses is
`MC_O_`, across both endpoints and both spans, with no `AT_O_` among them. So
each response is readable on its own: a request for LATISS came back full of
LSSTCam exposures.

Two further confirmations:

- Pre-refactor, `instrument=LATISS` and `instrument=LSSTCam` return
  **byte-identical payloads** for both endpoints — the parameter had no effect at
  all.
- `/exposure-entries` records also carry an explicit `"instrument"` field, and in
  the LATISS response every one of them reads `"LSSTCam"`.

After the refactor LATISS correctly returns zero, because
`ExposureEntriesService` and `ExposureFlagsService` filter on each cached
message's own `instrument` field. The LSSTCam files for both endpoints are `OK`,
which is the other half of the check: the data itself did not move, only which
instrument it is served under.

### `exposures__1day__LATISS.json`, `exposures__7day__LATISS.json`

**The endpoint was broken for LATISS and now works.** Before:
`HTTP 502 {"detail": "ConsDB query failed"}` on both spans. After: `HTTP 200`.

All twelve reported differences are the same event — `body.detail: removed`,
`status: 502 -> 200`, and the ten keys of a successful payload appearing where an
error body used to be.

The new payload was checked rather than assumed: every `exposure_name` prefixed
`AT_` (AuxTel — genuinely LATISS, not more LSSTCam leakage), all `day_obs`
values inside the window, `seq_num` monotonic within each night, no nulls across
`exposure_id`/`obs_start`/`exp_time`/`band`/`can_see_sky`, and both optional
sub-queries succeeded — `open_dome_error` and `time_accounting_error` are both
`null` and dome hours are present for every night.

### `exposures__7day__LSSTCam.json`

Same records, different order — and here the new order is a **fix**.

The old query ended `ORDER BY e.seq_num ASC`. But `seq_num` is a per-night
sequence that restarts each dayobs, so ordering by it alone interleaves the
nights — writing `(night, seq)`:

```
before: (1,1) (3,1) (2,1) (1,2) (3,2) ...
after:  (1,1) (1,2) (1,3) (1,4) (1,5) ...
```

`ExposuresService.collate_response` iterates dayobs in order and sorts each
night's records by `seq_num`, giving `(day_obs, seq_num)`. Verified directly: the
before payload is exactly sorted by `seq_num` alone and is *not* sorted by
`(day_obs, seq_num)`; the after payload is.

So the old ordering was intentional but wrong for multi-night ranges — the SQL
had no way to express that `seq_num` resets nightly. The 1-day file is `OK`
because with a single night the two orderings are identical.

### `jira-tickets__7day__LATISS.json`

Same tickets, different order — key sets identical, nothing gained or lost.

`JiraTicketsService` deduplicates through a dict keyed on ticket key while
iterating dayobs buckets in ascending order, so output order now follows
first-seen dayobs. The old path returned Jira's own search order.

The 1-day files are `OK` because a single dayobs bucket preserves the order the
tickets arrived in.

### `jira-tickets__7day__LSSTCam.json`

One ticket fewer. The remainder are in the same first-seen-dayobs order
described above, and the missing one is **upstream drift between the two capture
runs, not a refactor difference**.

The ticket was created long before the window. Its only claim on the range is
its last-update time, and the JQL — identical in both code paths — selects on
either:

```python
jql_query = (
    f"project = OBS {status_exclusions} "
    f'AND ((created >= "{start}" AND created < "{end}") '
    f'OR (updated >= "{start}" AND updated < "{end}"))'
)
```

Jira stores only the latest update time, so touching the ticket again moves
`updated` out of the window and the ticket leaves the result set entirely. It was
edited between the two captures.

Two independent checks that this is not the refactor. The earlier capture pair
returned the ticket on **both** sides, with the same new code that omits it now.
And its `system` field is not one the LSSTCam path's `INSTRUMENT_EXCLUDE_MAP`
filter matches on, so nothing in the service would have dropped it.

Worth knowing as a property of the endpoint regardless of the refactor: a ticket
whose only tie to a night is its last-update time silently disappears from that
night's view as soon as anyone edits it again.

### `multi-night-visit-maps__*.json` — all four files

**Entirely noise.** Verified independently of the structural differ: collecting
the multiset of every leaf value in each Bokeh document, ignoring position
completely, the two documents are identical on all four files, with **zero**
differing values.

The residual differences are Bokeh building the same document with a different
tree shape between processes. In every file two of the map's fixed polyline data
sources swap `js_property_callbacks` slots, and in the LSSTCam files a handful of
toolbar renderers are listed in a different order. Everything else sits in the
same place on both sides.

That this is per-process and not per-code is visible directly: those polyline
sources appear in a different order in each of the four captures taken,
including between two runs of the *same* code. The reported difference count is
therefore itself noise — it records how many slots happened to move, which is
why an earlier capture pair of the same code reported several times as many.

Two things make a rearrangement surface as differences at all:

- The structural differ compares by position, so a rearrangement reads as
  added/removed keys and changed list lengths.
- The tool's Bokeh id renumbering is itself traversal-order dependent — the same
  logical object can receive a different placeholder id on each side,
  manufacturing differences rather than removing them.

Neither affects the data. The `static-visit-map` files render the same underlying
visit query to PNG and are `OK` on all four captures, which is a useful
independent check on the same data.
