# Capture comparison

This file contains the results of running `scripts/capture_endpoints.py`
on the service both before and after the caching refactor. This fetches
each endpoint for both a 1 and 7 day block, and compares the responses.
A verdict of `OK` indicates that the endpoint produces a *byte-identical*
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


Note that the `before` capture was taken on 3 August and the `after` capture on
5 August.

## Findings

### `context-feed__1day.json`, `context-feed__7day.json`

The difference count is misleading: it is three lines describing thousands of
records, nearly all differing in a single field. Pairing on
`(time, name, script_salIndex)` — unique across every record on both sides — all
but a handful pair up, and the record counts reconcile exactly:

| | `before` | dropped | gained | `after` |
|---|---|---|---|---|
| 1-day | 7198 | 6 | 1 | 7193 |
| 7-day | 9319 | 6 | 0 | 9313 |

**1. Timestamps that serialized as the string `"NaT"` are now correctly `null`.**
This is the overwhelming majority of the changed fields, across all five
timestamp columns. The old path ran only `stringify_special_floats`, which
handles float NaN/Inf but not `pd.NaT`, so a null timestamp reached the client as
a literal string. `make_json_safe` now maps it to a real JSON `null`.

A fix, but a frontend-visible type change: a truthiness check saw a truthy
`"NaT"` before and gets `null` now. **This is the only change in the refactor
that requires a corresponding change in the frontend** — everything else in this
document is either invisible to it, filtered out client-side, or a reordering it
already re-sorts.

**2. Six records fetched from before the requested window are dropped.** The same
six in both spans, all of them configuration rows timestamped one to two days
before the range starts.

They arrive unrequested. `get_consolidated_messages` deliberately reaches back
for the last known state before `t_start`. The old code forwarded all configuration
rows. The new per-dayobs bucketing files each of them under its own dayobs, 
which the service never requests, so they are dropped.

**Not a user-visible loss.** `ContextFeed.jsx` filters every row against
`selectedTimeRange`, which defaults to `fullTimeRange`, derived directly from the
dayobs in the URL. These records were fetched, serialized, sent and then
discarded client-side. They have never been displayed.

**3. The 1-day span gains a record, and it is the dayobs-boundary mitigation working.**
The gained row is an Image Acquired row for `MC_O_20260713_000961`, indexed 19
seconds before the noon rollover that ends the range.

Image rows are *selected* on the EFD publish timestamp and then *re-indexed* onto
`timestampAcquisitionStart`, and acquisition precedes publication by the exposure
plus readout. An image acquired 19 seconds before noon is published after it, so
the old query — which stopped dead at the boundary — never saw it, even though
the row belongs inside the range. The trailing margin on each run's query lets
the run see events that belong to it but land late, and the bucketing still files
them by their own dayobs, so nothing leaks into a neighbouring day.

Two further consequences of that one record, both in the 1-day file:

- The last Task Change row's `timestampProcessEnd` moves, because
  `collate_response` gives the final task change `records[-1]["time"]` and the
  response now has one more trailing record.
- A script that the old response reported as still running is now reported as
  finished. `get_script_state` aggregates `finalScriptState` and
  `timestampProcessEnd` with `max` over the messages *in the query window*; this
  script started at 11:23 and finished at 12:20, straddling noon, so the old
  window closed with it mid-flight and its end timestamp left at the
  `TIMESTAMP_ZERO` sentinel that `rubin_nights` uses for an unset value. The
  margin covers the overrun and the row now carries its real terminal state.

The 7-day span gains nothing because its range already extends past both events.

**4. Whole-number exposure times rendered without a decimal**, on 12 Image
Acquired rows in the 7-day file only — every AuxTel image of `20260714`.

`rubin_nights` builds the string by interpolating the raw cell:

```python
def make_config_col_for_image(x: pd.Series) -> str:
    return f"exp {x.exposureTime} // dark {x.darkTime} // open {x.measuredShutterOpenTime} "
```

and the frame comes from `InfluxQueryClient._to_dataframe`, which does
`pd.DataFrame(series.get("values", []), columns=series["columns"])`. InfluxDB's
JSON encoder writes a float holding a whole number as a JSON integer, so pandas
infers each column's dtype from the rows in *that one query*: all integer gives
`int64` and renders without a decimal point, any fractional value gives `float64`
and renders with one. `darkTime` is fractional throughout, stays `float64`, and
is identical across the whole diff — only `exp` and `open` move, which is what
makes this a per-column dtype effect rather than a change to the string builder.

### `data-log__1day__LSSTCam.json`, `data-log__7day__LSSTCam.json`

Identical records, different order — same multiset both sides. The new order is
deliberate.

`DataLogService.collate_response` now sorts:

```python
records = flatten_within_dayobs(data, "seq_num")
```

which walks the dayobs buckets in ascending order and sorts each night's records
by `seq_num`, giving `(day_obs, seq_num)`. Verified directly: both `after`
payloads are exactly in that order and neither `before` payload is in any order
at all — the 1-day payload is sorted by neither `seq_num` nor `(day_obs,
seq_num)`, and nor is the 7-day.

Before the refactor there was no ordering to speak of. Neither the old query nor
the old Python path sorted, so `/data-log` returned whatever Postgres produced.

The LATISS files are `OK` because Postgres happened to return those rows in
`(day_obs, seq_num)` order already. That is a coincidence of the query
plan rather than a guarantee.

`/exposures` and `/data-log` read the same cache entry and now agree on ordering;
both return 2650 LSSTCam records for the 7-day span.

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
parameter — its filter is `instruments`, an array of instrument names.
Unrecognised query parameters are ignored rather than rejected, so the
request succeeded and returned every instrument's messages.

Two further confirmations:

- Pre-refactor, `instrument=LATISS` and `instrument=LSSTCam` return
  **byte-identical payloads** for both endpoints on both spans — the parameter
  had no effect at all.
- `/exposure-entries` records also carry an explicit `"instrument"` field, and in
  the LATISS response every one of them reads `"LSSTCam"`.

After the refactor LATISS correctly returns zero records. The LSSTCam files for both endpoints are `OK`,
which is the other half of the check: the data itself did not move, only which
instrument it is served under. It also says the test week holds no LATISS
exposure-log records at all — otherwise the LSSTCam responses, which used to
carry every instrument, would have shrunk too.

### `exposures__1day__LATISS.json`, `exposures__7day__LATISS.json`

**The endpoint was broken for LATISS and now works.** Before:
`HTTP 502 {"detail": "ConsDB query failed"}` on both spans. After: `HTTP 200`.

All twelve reported differences are the same event — `body.detail: removed`,
`status: 502 -> 200`, and the ten keys of a successful payload appearing where an
error body used to be.

**The cause is the column list, and the captures prove it.** The old `/exposures`
query named quicklook columns explicitly:

```sql
SELECT e.exposure_id, ..., q.zero_point_median, q.visit_id,
       q.pixel_scale_median, q.psf_sigma_median
FROM cdb_{telescope}.exposure e
LEFT JOIN cdb_{telescope}.visit1_quicklook q ON e.exposure_id = q.visit_id
```

Three of those four columns do not exist for LATISS. Now, `ConsdbExposuresAdapter._fetch_run` \
selects `e.*, q.*` instead, and
`ExposuresService.collate_response` projects the same 24 columns in Python with
`record.get(column)`. A column that is not there becomes `null` rather than a
failed query. 

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
night's records by `seq_num`, giving `(day_obs, seq_num)`. 

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

One ticket fewer. The remainder are in the same first-seen-dayobs order described
above, and the missing one is **upstream drift between the two capture runs, not
a refactor difference**. One of the tickets in the capture set was updated between
the before and after runs.


### `multi-night-visit-maps__*.json`

**Entirely noise.** Bokeh document creation is non-deterministic: comparing
the multiset of every leaf value in each Bokeh document, ignoring position
completely, the two documents are identical on all four files, with **zero**
differing values.
