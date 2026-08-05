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

24 identical, 4 differing only in list order, 15 differing.

Note that the `before` capture was taken on 3 August and the `after` capture on
5 August, so a difference can also be upstream data changing between the two
runs; where that is the cause, the section says so.

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

Note that the `1day` file covers two nights, not one. `ContextFeedService.handle`
treats `dayObsEnd` as inclusive, where every other service converts it with
`add_or_subtract_dayobs_days(day_obs_end, -1)` first. The old code did the same
thing, so both sides agree and it is not a refactor difference — but it is why
this file reaches a day further than `data-log__1day` does, and the sections
below depend on that.

**1. Timestamps that serialised as the string `"NaT"` are now correctly `null`.**
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
for the last known state before `t_start` with five `select_top_n(num=1)` calls —
`conf_start` and `deps_start` once per `queue_index` (`scriptqueue.py:115,159`,
`queue_index` defaulting to `[1, 2]`), plus `obsenv_start`
(`scriptqueue.py:200`). The sixth is an obsenv row that is not a lookback at all:
the obsenv time series is queried from `sched_config.index[0]` rather than from
`t_start`, so it starts at the earliest scheduler lookback and sweeps up whatever
obsenv rows fall in between. Here that is one `Obsenv Check`.

The old code forwarded all six. The new per-dayobs bucketing files each of them
under its own dayobs, which the service never requests, so they are dropped.

**Not a user-visible loss.** `ContextFeed.jsx` filters every row against
`selectedTimeRange`, which defaults to `fullTimeRange`, derived directly from the
dayobs in the URL. These records were fetched, serialised, sent and then
discarded client-side. They have never been displayed.

They are load-bearing upstream, though, which is the argument against fixing this
by narrowing the query. `get_scheduler_configs` labels each obsenv row `Obsenv
Check` or `Obsenv Update` by comparing it against its predecessor in the query,
so `obsenv_start` is what makes the first in-window row correct. Dropping the row
after that comparison has been made is safe; never fetching it would not be.

**3. The 1-day span gains a record, and it is the dayobs-boundary fix working.**
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
infers each column's dtype from the rows in *that one query*: all integral gives
`int64` and renders without a decimal point, any fractional value gives `float64`
and renders with one. `darkTime` is fractional throughout, stays `float64`, and
is identical across the whole diff — only `exp` and `open` move, which is what
makes this a per-column dtype effect rather than a change to the string builder.

`get_exposure_info` queries MainTel, ComCam and AuxTel separately, so the dtype
is decided per camera per query. That already made the `before` payload
inconsistent between cameras: every MainTel row renders integral and every AuxTel
row fractional, in a single response. What the refactor adds is inconsistency
*within* a camera. Splitting the range at a run boundary put these 12 AuxTel rows
in a chunk whose exposure times were all integral, so the same column renders
both ways in one `after` payload — 1643 fractional and 12 integral.

This needs the upstream formatting fixed; nothing on this side can do better than
choose which nights share a query.

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
That it changed at all tracks the added join: `EFD_FIELDS` is non-empty only for
`lsstcam`, so only LSSTCam picks up the third join to the transformed-EFD table,
and only the LSSTCam files reordered.

The LATISS files are `OK` because Postgres happened to return those rows in
`(day_obs, seq_num)` order already — both `before` LATISS payloads are exactly
sorted, so the new explicit sort is a no-op for them. That is a coincidence of
the query plan rather than a guarantee, which is the argument for sorting
explicitly.

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
parameter — its filter is `instruments`, an array ("Names of instruments (e.g.
LSSTCam). Repeat the parameter for each value.", per the service's OpenAPI
schema). Unrecognised query parameters are ignored rather than rejected, so the
request succeeded and returned every instrument's messages.

Each response is readable on its own. The `obs_id` prefix names the telescope —
`MC_O_` for MainTel/LSSTCam, `AT_O_` for AuxTel/LATISS — and **every** record
removed from the LATISS responses is `MC_O_`, across both endpoints and both
spans, with no `AT_O_` among them. So a request for LATISS came back full of
LSSTCam exposures.

Two further confirmations:

- Pre-refactor, `instrument=LATISS` and `instrument=LSSTCam` return
  **byte-identical payloads** for both endpoints on both spans — the parameter
  had no effect at all.
- `/exposure-entries` records also carry an explicit `"instrument"` field, and in
  the LATISS response every one of them reads `"LSSTCam"`.

After the refactor LATISS correctly returns zero, because
`ExposureEntriesService` and `ExposureFlagsService` filter on each cached
message's own `instrument` field. The LSSTCam files for both endpoints are `OK`,
which is the other half of the check: the data itself did not move, only which
instrument it is served under. It also says the test week holds no LATISS
exposure-log records at all — otherwise the LSSTCam responses, which used to
carry every instrument, would have shrunk too.

One thing to read correctly rather than as an anomaly: `/exposure-flags` returns
the same 11 records for the 1-day and the 7-day span, while `/exposure-entries`
grows from 11 to 23. Nights after the first do carry exposure-log entries; none
of them is flagged. The two `exposure-flags` LATISS files being identical to each
other is that, not a capture taken twice.

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

Three of those four columns do not exist for LATISS. `/data-log` selects `e.*,
q.*` and succeeded for LATISS in the same `before` capture, so its payload is a
direct listing of what the joined LATISS schema actually has: 91 columns against
LSSTCam's 216, and of the four columns above only `visit_id` is among them.
`zero_point_median`, `pixel_scale_median` and `psf_sigma_median` are absent. The
old `/exposures` SQL therefore could not parse against `cdb_latiss`, which is the
`ConsdbQueryError` behind the 502 — and it also rules out the alternatives, since
ConsDB, the credentials and the `cdb_latiss` schema were all plainly reachable in
that same run.

`ConsdbExposuresAdapter._fetch_run` selects `e.*, q.*` instead, and
`ExposuresService.collate_response` projects the same 24 columns in Python with
`record.get(column)`. A column that is not there becomes `null` rather than a
failed query. The `after` LATISS payload matches that exactly: all four quicklook
columns are null in every record, including `visit_id`, which exists but never
matches — the `before` `/data-log` capture shows `visit_id` null on all 739
LATISS rows too, so the join finds nothing for this instrument either way.

The rest of the new payload was checked rather than assumed: every
`exposure_name` prefixed `AT_` (AuxTel — genuinely LATISS, not more LSSTCam
leakage), all `day_obs` values inside the window, `seq_num` ascending within each
night, no nulls across `exposure_id`/`obs_start`/`exp_time`/`band`/`can_see_sky`,
and both optional sub-queries succeeded — `open_dome_error` and
`time_accounting_error` are both `null`.

Worth carrying into review: the new path cannot distinguish "this column does not
exist" from "this column is null". That is what makes it work here, and it is
also what would let a column silently disappearing upstream show up as nulls in
the response rather than as an error.

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
`before` payload is exactly sorted by `seq_num` alone and is *not* sorted by
`(day_obs, seq_num)`; the `after` payload is the reverse.

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
a refactor difference**.

`OBS-1450` was created in December 2025, seven months before the window. Its only
claim on the range is its last-update time, and the JQL — identical in both code
paths — selects on either:

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

That it is not the refactor is checkable from the payload: its `system` field is
`["Control and Monitoring Software", "Scheduler"]`, neither of which the LSSTCam
path's `INSTRUMENT_EXCLUDE_MAP` filter matches on, so nothing in the service
would have dropped it.

Worth knowing as a property of the endpoint regardless of the refactor: a ticket
whose only tie to a night is its last-update time silently disappears from that
night's view as soon as anyone edits it again.

### `multi-night-visit-maps__*.json` — all four files

**Entirely noise.** Verified independently of the structural differ: collecting
the multiset of every leaf value in each Bokeh document, ignoring position
completely, the two documents are identical on all four files, with **zero**
differing values — 33 324 leaves for `1day__LATISS`, 62 207 for `1day__LSSTCam`,
40 420 and 100 024 for the 7-day pair, matching exactly on both sides.

**One thing moved, and it accounts for every difference line.** The four
footprint-outline polylines are permuted. Their coordinate lists hold 854, 561,
1526 and 55 points; the `before` document carries them in that order and the
`after` document in the order 55, 1526, 854, 561. Nothing else in the equatorial
sliders' callback list moves — slots 4 through 24, which include the graticules,
ecliptic, galactic plane, horizons and every visit-patch source, sit at identical
positions on both sides.

The rest follows from that one permutation:

| Lines | What |
|---|---|
| 12 | the four coordinate sources: one `length`, one `removed` and one `added` line each |
| 24 | the same four renderers in both map panels, ×3 glyph variants (`glyph`, `muted_glyph`, `nonselection_glyph`) |
| 2 | LSSTCam only: a hover tool's `renderers` list, reported as `reordered: same 4 elements` |

The renderer lines read as `line_color: added` at positions 0 and 1 and
`removed` at 2 and 3 because two of the four outlines are drawn with an explicit
`darkgray` and two inherit the default. Bokeh serialises only non-default
properties, so when the four change places the property appears to move with
them. The renderer list itself is otherwise untouched: all fifteen entries hold
the same data sources in the same positions on both sides.

**The ordering is hash-seed dependent, and that is demonstrable.**
`_build_visit_map` gets its regions from
`get_current_footprint(nside)` (`services/visit_maps.py:222`), which returns a
per-healpix array of region labels, and passes them to
`add_footprint_outlines`. Reducing that array to its distinct labels through a
`set` gives nine entries whose iteration order changes with every
`PYTHONHASHSEED`:

```
$ PYTHONHASHSEED=1 … → ['', 'nes', 'virgo', 'scp', 'LMC_SMC', 'euclid_overlap', 'lowdust', …]
$ PYTHONHASHSEED=2 … → ['', 'euclid_overlap', 'lowdust', 'nes', 'dusty_plane', 'bulgy', …]
$ PYTHONHASHSEED=3 … → ['', 'lowdust', 'euclid_overlap', 'dusty_plane', 'scp', 'LMC_SMC', …]
```

That the outline order follows this set rather than a sorted or fixed sequence is
inferred rather than read — `schedview` is not importable in this checkout — but
it matches the observed behaviour exactly, including why it is stable *within* a
capture and varies *between* captures. All four `after` files agree with each
other and all four disagree with the `before` files in the same way, which is
what a single process-wide seed produces. `worker_pool_mixin.py:195` starts the
map workers with the `forkserver` method, so every worker in a server's lifetime
inherits one seed and agrees with its siblings; a restart draws a new one.

Setting `PYTHONHASHSEED` for the server would make the document reproducible
across restarts. Nothing renders differently either way — the outlines are drawn
from the same coordinates in either order — but it matters if anything downstream
ever wants to cache or `ETag` this response, and it would stop this endpoint
producing diff noise in future comparisons.

**Two things make a rearrangement surface as differences at all:**

- The structural differ compares by position once it cannot match two lists as
  multisets, so a rearrangement reads as added/removed keys and changed list
  lengths.
- It cannot match them as multisets here because the tool's Bokeh id renumbering
  is itself traversal-order dependent — the same logical object receives a
  different placeholder id on each side, manufacturing differences rather than
  removing them.

So the reported difference count is not a measure of anything: it records how many
slots happened to move and how far the id renumbering propagated from them.

## What this comparison does not cover

Things that could make a capture non-identical, or identical without meaning
much, that the table cannot show.

**Two of the four `static-visit-map` `OK` verdicts are vacuous.** Both LATISS
files return `{"static_map": null}` on both sides — the endpoint produces no map
for that instrument. Only the LSSTCam pair is a real check, and it is a strong
one: the capture replaces the base64 payload with a SHA-256 of the decoded PNG
bytes, and both hashes match across the two-day gap. That is the independent
evidence that the underlying visit data did not drift; it just does not extend to
LATISS.

**The Bokeh id canonicaliser rewrites more than Bokeh ids.**
`capture_endpoints.py:176` declares `ID_KEYS = {"id", "root_id", "target_id"}`
and `canonicalise_response` carries the comment "Ids are only meaningful under
the keys that carry them; walking the whole payload would rewrite any string that
happens to look like one" — but `canonicalise` never consults `ID_KEYS` and does
walk the whole payload. The manifests show the transform firing on
`narrative-log` (all four files), `night-reports` (both) and `exposure-entries`
(the LSSTCam files), where it renumbers the `id` and `parent_id` UUIDs of log
messages and night reports.

Those files are all `OK`, and that verdict does not cover their ids: two
different UUIDs in the same position both canonicalise to the same `id-N`.
Because the numbering is first-seen, inserting or removing one record also shifts
every id after it, which manufactures differences rather than removing them.
Fixing it means threading the key down and only rewriting under `ID_KEYS` — which
would need `parent_id` adding to that set as well.

**Render timestamps are masked to a single constant.** Every 19-digit numeric
string in the 2020–2100 range becomes `<render-timestamp>`, so a real difference
between two such values is invisible. That is the intended trade for Bokeh's
build-time stamp, but unlike the id mapping it is not a numbered mapping and
cannot be audited after the fact.

**Only status and body are compared.** Headers are not captured, so content type,
caching headers and the still-missing response compression are all outside this
evidence. Object keys are sorted on write, so key ordering is not compared either
— deliberate, since it carries no meaning, but it means "byte-identical" is a
claim about the parsed document rather than the wire format.

**Twenty difference lines per file, three examples per line.** `--max-lines`
defaults to 20, so the visit-map files print only 20 of their 36 and 38
differences, and `_summarise` shows three sample records per added/removed set.
Both of the context-feed files report their entire content in three lines for
that reason; the findings above come from the payloads, not from the summary.

**A response is a function of cache state, not just of the request.**
`capture_endpoints.py` issues the 1-day call before the 7-day call for each
endpoint, so every 7-day `after` response is assembled from an entry built by the
1-day query plus a second entry covering the remaining run. A cold cache per
request, or a cache pre-populated by `RefreshWorker` with single-day entries,
composes the same range out of differently-bounded queries. That is invisible for
every endpoint here except `/context-feed`, where it decides how exposure times
are rendered — but it is a property of the design rather than of this capture,
and a future comparison run in a different order can legitimately differ from
this one.
