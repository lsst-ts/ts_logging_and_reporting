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
| `jira-tickets__7day__LSSTCam.json` | ORDER ONLY | 1 |
| `multi-night-visit-maps__1day__LATISS.json` | DIFFERS | 36 |
| `multi-night-visit-maps__1day__LSSTCam.json` | DIFFERS | 38 |
| `multi-night-visit-maps__7day__LATISS.json` | DIFFERS | 36 |
| `multi-night-visit-maps__7day__LSSTCam.json` | DIFFERS | 38 |
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

## Findings

### `context-feed__1day.json`, `context-feed__7day.json`

The difference count is misleading: it is three lines describing thousands of
records, nearly all differing in a single field. Pairing on
`(time, name, script_salIndex)`, **7192 of 7198 records pair up**.

Three separate causes.

**1. `"NaT"` became `null` — 4567 fields.** The old path ran only
`stringify_special_floats`, which handles float NaN/Inf but not `pd.NaT`, so a
null timestamp serialised as the literal string `"NaT"`. `make_json_safe` now
maps it to a real JSON `null`. A fix, but a frontend-visible type change: a
truthiness check saw a truthy `"NaT"` before and gets `null` now. Affects
`timestampConfigureEnd`/`Start` (3957 each), `timestampRunStart` (333),
`timestampProcessEnd` (309), `timestampProcessStart` (11).

**2. Microsecond to nanosecond precision — 23 records.**
`...290646+00:00` became `...290645999+00:00`.

This is not a formatting change; the values have different provenance.
`ContextFeedService.collate_response` recomputes `timestampProcessEnd` for
"Task Change" rows by copying another record's `time`
(`services/context_feed.py`, lines 60–67; see §3 of `REFACTOR_ODDITIES.md` for
why the recompute exists at all):

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

Every affected record is a Task Change row, and every new value matches another
record's `time` verbatim. The count is 23 of the 24 Task Change rows in the
1-day capture: the odd one out is the last, which takes the `else` branch and
copies `records[-1]["time"]`, a value that happened to already match.

**Only ISO-string fields are affected.** Every field the context feed returns is
a string, integer or null; there are no numeric epoch timestamps in it. The
comparable numeric field elsewhere is `/obs-status`'s `time_ms`, computed as
`frame["time"].astype("int64") // 1_000_000` — flooring to milliseconds discards
everything below the millisecond, so sub-microsecond precision cannot reach it.
Both `/obs-status` captures are byte-identical, `time_ms` included.

This ns precision is lost on the frontend when converting to `Date()` and is
unrelated to the ns/µ obs-status issue usdf-dev recently suffered.

**3. Six records dropped, all predating the window.** All are
`finalStatus: "Configuration"` — `Obsenv`, `Obsenv Check`,
`Scheduler configuration`, `Scheduler dependencies` — timestamped 10–11 July for
a window starting noon on the 12th. They arrive unrequested:
`get_consolidated_messages` deliberately reaches back for the last known
configuration via `select_top_n(num=1, time_cut=...)`. The old code forwarded
whatever came back; the new per-dayobs bucketing files them under their own
dayobs, which the service never requests.

**Not a user-visible loss.** `ContextFeed.jsx` filters every row against
`selectedTimeRange`, which defaults to `fullTimeRange`, derived directly from the
dayobs in the URL. These records were fetched, serialised, sent and then
discarded client-side. They have never been displayed.

**One further record is genuinely lost, in the 7-day capture only.**
`MC_O_20260713_000961`, an Image Acquired row reporting
`2026-07-14T11:59:41.634` — inside the requested range, so the frontend filter
would have shown it.

The cause is a seam in `rubin_nights`. Image-acquisition rows are *selected* by
the EFD's publish timestamp, then *re-indexed* onto `timestampAcquisitionStart`:

```python
image_acquisition_mt = efd_client.select_time_series(topic, fields, t_start, t_end)
...
image_acquisition_mt.index = image_acquisition_mt["timestampAcquisitionStart"].copy()
```

Acquisition start precedes publication by the exposure plus readout, so a row can
be selected by one window and report a time outside it. Only 3 of the 11 sources
`get_consolidated_messages` assembles do this — the other 8 keep the EFD index —
and nothing clamps the result to `[t_start, t_end]`. The underlying values are
unix-TAI and *are* correctly converted to UTC, so the 37-second leap-second
offset is not the culprit; the publish lag is.

For a single query this can only pull *extra* rows in, never lose one. Losing
requires stitching adjacent windows and discarding what falls outside each, which
is what the per-dayobs cache does: the run owning 20260713 closed its window at
noon before this row was published, and the run starting at noon on the 14th
selected it, bucketed it to 20260713, then dropped it as out of range.

Worth knowing:

- The loss is **cache-state dependent**. Whether a day is a run boundary depends
  on what is already warm, so the same request can return different records at
  different times. That is the more troubling property, though invisible to a
  user who cannot see the row either way.
- The fix is a margin on each run's trailing query window, sized to exceed the
  worst-case publish lag. Widening the clamp instead does **not** work: runs are
  built from cache misses, so a run emitting a bucket for a day it does not own
  would overwrite a complete cached entry with a fragment.
- Not fixed. This issue warrants further discussion, including with users,
  about what the correct resolution is.

### `data-log__1day__LSSTCam.json`, `data-log__7day__LSSTCam.json`

Identical records, different order — 969 and 2650 elements, same multiset both
sides.

Neither the old nor the new query has an `ORDER BY`, and neither path sorts in
Python, so `/data-log` ordering has always been whatever Postgres returned.
Both versions join `exposure LEFT JOIN visit1_quicklook`; what changed is that
the new query adds a *third* join to the transformed-EFD table, which is enough
to change the plan and therefore the row order.

The evidence for that is the instrument split. `EFD_FIELDS` is non-empty only for
`lsstcam`, so only LSSTCam picks up the extra join — and only the LSSTCam files
reordered. LATISS returned 739 and 1655 rows in **exactly** the same order on
both sides. So this is not general query nondeterminism; it tracks the added
join.

Note the asymmetry this leaves. `/exposures` and `/data-log` read the same cache
entry, and `/exposures` now sorts explicitly while `/data-log` does not. If the
frontend depends on `/data-log` ordering, `DataLogService` should sort the way
`ExposuresService` does — the ordering it currently gets is incidental and could
change again with the next plan change.

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

The `obs_id` prefix names the telescope — `MC_O_` for MainTel/LSSTCam, `AT_O_`
for AuxTel/LATISS — and **every** record removed from the LATISS responses is
`MC_O_`, across both endpoints and both spans (11, 23, 11, 11 records; zero
`AT_O_` among them). So each response is readable on its own: a request for
LATISS came back full of LSSTCam exposures.

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

The new payload was checked rather than assumed. For the 7-day span: 1655
records, every `exposure_name` prefixed `AT_` (AuxTel — genuinely LATISS, not
more LSSTCam leakage), all `day_obs` values inside the window, `seq_num`
monotonic within each night, no nulls across
`exposure_id`/`obs_start`/`exp_time`/`band`/`can_see_sky`, and both optional
sub-queries succeeded — `open_dome_error` and `time_accounting_error` are both
`null` and dome hours are present for all three nights.

### `exposures__7day__LSSTCam.json`

Same 2650 records, different order — and here the new order is a **fix**.

The old query ended `ORDER BY e.seq_num ASC`. But `seq_num` is a per-night
sequence that restarts each dayobs, so ordering by it alone interleaves the
nights:

```
before: (20260712,1) (20260714,1) (20260713,1) (20260712,2) (20260714,2) ...
after:  (20260712,1) (20260712,2) (20260712,3) (20260712,4) (20260712,5) ...
```

`ExposuresService.collate_response` iterates dayobs in order and sorts each
night's records by `seq_num`, giving `(day_obs, seq_num)`. Verified directly: the
before payload is exactly sorted by `seq_num` alone and is *not* sorted by
`(day_obs, seq_num)`; the after payload is.

So the old ordering was intentional but wrong for multi-night ranges — the SQL
had no way to express that `seq_num` resets nightly. The 1-day file is `OK`
because with a single night the two orderings are identical.

Note this is the opposite situation to `/data-log`, which never had an
`ORDER BY` at all. The two endpoints now read the same cache entry but came
from different queries before the refactor.

### `jira-tickets__7day__LATISS.json`, `jira-tickets__7day__LSSTCam.json`

Same tickets, different order — 2 and 8 respectively, key sets identical, nothing
gained or lost.

`JiraTicketsService` deduplicates through a dict keyed on ticket key while
iterating dayobs buckets in ascending order, so output order now follows
first-seen dayobs. The old path returned Jira's own search order.

The 1-day files are `OK` because a single dayobs bucket preserves the order the
tickets arrived in.

### `multi-night-visit-maps__*.json` — all four files

**Entirely noise.** Verified independently of the structural differ: collecting
the multiset of every leaf value in each Bokeh document, ignoring position
completely, the two documents are identical — 100024 values on both sides for the
7-day LSSTCam case, and likewise for the other three.

The only genuinely differing values were two per file, decoding as
nanosecond-epoch render timestamps that match the capture times to the second
(07:25:07 versus 08:25:37 for 1-day LSSTCam, and so on for the rest). The capture
tool masks these, so they do not appear in the counts above.

The residual differences are Bokeh building the same document with a different
tree shape between processes: `line_color` lands on `renderers[0]` in one and
`renderers[2]` in the other, and the four graticule arrays occupy different
callback slots. Two things make that surface as differences:

- The structural differ compares by position, so a rearrangement reads as
  added/removed keys and changed list lengths.
- The tool's Bokeh id renumbering is itself traversal-order dependent — the same
  logical object can receive a different placeholder id on each side,
  manufacturing differences rather than removing them.

Neither affects the data. The `static-visit-map` files render the same underlying
visit query to PNG and are `OK` on all four captures, which is a useful
independent check on the same data.


