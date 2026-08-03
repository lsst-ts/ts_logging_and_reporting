# Refactor Oddities

Companion notes for reviewers of the backend caching refactor.

This document collects the things a reviewer would not find by reading the diff:
moves git cannot detect, behaviour that changed for reasons that are not local to
the change, decisions that departed from the plan, and the handful of places where
the code is deliberately left imperfect.

It is not a summary of the refactor. For the architecture itself see
`doc/service-adapter-infrastructure.md`; for the reasoning behind it see
`review/BACKEND_REFACTOR_PLAN.md`, and for the plan as first written
`review/BACKEND_REFACTOR_PLAN_ORIGINAL.md` (both deleted before merge — see
[§14](#14-divergence-from-the-original-refactor-plan)). See also
`review/capture-compare.md` for a comparison of the exact output response
for each endpoint before and after the refactor, and `review/performance/COMPARE.md`
for a comparison of the performance before and after the refactor.

---

## Contents

1. [How to review and test this change](#1-how-to-review-and-test-this-change)
2. [The authentication model changed](#2-the-authentication-model-changed)
3. [Logic that changed because of caching](#3-logic-that-changed-because-of-caching)
4. [Structure and layout](#4-structure-and-layout)
5. [Function moves git will not show you](#5-function-moves-git-will-not-show-you)
6. [Data-source changes](#6-data-source-changes)
7. [Error handling and the frontend contract](#7-error-handling-and-the-frontend-contract)
8. [Middleware](#8-middleware)
9. [Caching, configuration and operations](#9-caching-configuration-and-operations)
10. [Logging](#10-logging)
11. [Deletions](#11-deletions)
12. [Serialisation fixes made in passing](#12-serialisation-fixes-made-in-passing)
13. [Known gaps and deferred work](#13-known-gaps-and-deferred-work)
14. [Divergence from the original refactor plan](#14-divergence-from-the-original-refactor-plan)

---

## 1. How to review and test this change

### The history you are reading is a reconstruction

The commit history presented for review was not the history in which this work was
developed. The work was built incrementally on `experiemental/cache-refactor` over
several weeks, in an order driven by what was learnable next rather than by what is
readable. That branch is retained, unmodified, as the real record — use it for
`git blame` and for any archaeology about why something is the way it is.

What you are reviewing is the finished tree replayed forward from `develop` as a
stack of branches, each slice landing whole files in their final state. The
reconstruction is verified by the end-state tree being byte-identical to the
reviewed source branch. No commit in the reconstruction contains an intermediate
version of a file that a later commit revises.

### Do not run the tests on the branch you are reviewing

**Interstitial commits are not working states, by design.** From the commit that
splits `utils.py` until the cleanup branch, the new `utils/` package shadows the
legacy `utils.py` module (Python resolves a package before a module of the same
name), so five legacy test modules fail collection for roughly forty commits:

```
tests/test_utils.py          tests/test_api_endpoints.py
tests/test_services.py       tests/test_dome_hours.py
tests/test_almanac_service.py
```

`pytest tests/` is therefore non-zero through the middle of the stack. This was
accepted deliberately: the alternative was deleting `utils.py` in the same commit
that creates the package, which would have made the move undiffable file-to-file.

There is one more: five of the thirty-two tests in `tests/test_base_adapter.py`
import `MutableDataMixin`, which arrives one commit after the base adapters do.
Those five are red for exactly one commit.

**Run manual and automated testing against the tip of the original
`experiemental/cache-refactor` branch**, even while reviewing a part-way slice. A
red test on a slice tells you nothing — it is more likely to be one of the two
documented cases above than a real defect. Per-slice, the new tests do give a clean
signal:

```
pytest tests/adapters tests/services tests/utils
```

### Two rename detections are misleading

Git reports `web_app/services/scheduler_service.py → services/visit_maps.py` as a
51%-similar rename. It is substantially a rewrite; treat it as new code.

Git also reports `tests/test_all_sources.py → utils/__init__.py` as a 54% rename.
The two files are unrelated — one is a deleted legacy test module, the other a new
package marker — but this is not a git defect, and it is worth understanding
because the same trap catches any small file in this repository.

Both files are mostly the 21-line GPL header that goes at the top of every file
here. `utils/__init__.py` is 1143 bytes, of which roughly 1000 is that header;
`test_all_sources.py` is 1738 bytes containing the same 1000. Twenty lines are
byte-identical. The two genuinely are ~54% the same content — git is measuring
accurately, the content it is measuring is just boilerplate. `utils/__init__.py`
has four non-boilerplate lines, making it the smallest added file in the change
and the most exposed to this.

The one file handled as a genuine, intentional move is
`tests/test_get_time_accounting.py → tests/services/test_time_accounting_helpers.py`
(81% similar), where preserved blame is worth having.

### Running the performance tests

`scripts/perf_test.py` is the harness the before/after numbers were gathered with.
It has three modes: `baseline` (run against pre-refactor code, where nothing is
cached and every request is cold by construction), `after` (run against the
refactor, sweeping cold / hot / partial / burst cache states per endpoint), and
`compare` (join two result files into a before/after table).

A full before/after therefore means two runs, on two different checkouts:

```
git checkout <merge-base>          # pre-refactor
run_logging_and_reporting
python scripts/perf_test.py baseline --out review/performance/BEFORE.json

git checkout experiemental/cache-refactor
run_logging_and_reporting          # plus Redis and the refresh worker
python scripts/perf_test.py after --out review/performance/AFTER.json

python scripts/perf_test.py compare review/performance/BEFORE.json \
    review/performance/AFTER.json
```

All three files are kept: the two raw result sets and the joined table, which is
`review/performance/COMPARE.md`. This comparison file also contains commentary
and analysis of the results.

Four things will invalidate the comparison if you get them wrong:

- **Use the same `--day-start` and `--instrument` for both runs.** Both default to
  a fixed, well-populated historical week, so the safe move is to pass neither —
  the captures in `review/performance/` were taken on the defaults. `compare`
  warns on mismatched ranges but cannot correct for them.
- **Point at uvicorn directly (port 8080), never at nginx**, or the proxy cache
  measures itself rather than the backend.
- **`after` mode issues `FLUSHDB` between runs.** Never point it at a shared or
  production Redis.
- The p50s alone do not prove that a partial load fetches only the missing days —
  they are consistent with it, but confirming it means counting upstream requests in
  the server log.

**Budget about four hours per run — roughly eight for a full before/after.** That is
mostly deliberate idling, not work: each scenario is 50 timed runs with `--pause`
(0.5 s) between them, and each burst scenario is 10 rounds of 10 simultaneous
requests with `--burst-pause` (5 s) between rounds, across seven endpoints and eight
scenarios each, plus `/block-details`. Every cold and partial scenario really does
hit ConsDB, the EFD, Jira and Zephyr on every single run — the pauses are there so a
measurement run does not present as a sustained load spike to production upstreams
that other people depend on.

So do not shrink `--pause` or `--burst-pause` to make a run fit in an afternoon.
If you need a faster signal, narrow the work instead: `--endpoints exposures
obs-status` to scope to the endpoints you changed, or `--runs 15` to trade
distribution quality for time. Both keep the request spacing intact.

The harness and the captures it produced are both removed before merge
([§11](#11-deletions)). Recover them from `experiemental/cache-refactor` if a later
change needs re-measuring.

### Verifying the endpoint outputs are unchanged

Performance is only half the claim. The other half — that the refactor did not
change *what* the API returns — is what `scripts/capture_endpoints.py` exists to
demonstrate, and `review/capture-compare.md` holds the result along with 
analysis and commentary.

It calls every data endpoint and writes one JSON file per call: each dayobs
endpoint over a 1-day and a 7-day range, each instrument-taking endpoint once per
instrument, and `/block-details` once against a fixed set of BLOCK keys that were
harvested from real `/data-log` data.

```
# on the pre-refactor checkout
python scripts/capture_endpoints.py capture --out <somewhere>/capture-before

# on experiemental/cache-refactor
python scripts/capture_endpoints.py capture --out <somewhere>/capture-after

python scripts/capture_endpoints.py compare <somewhere>/capture-before \
    <somewhere>/capture-after
```

`compare` exits non-zero if anything differs, and prints a per-call table of
verdicts. `review/capture-compare.md` holds that table together with a written
explanation of every difference in it.

Unlike the performance run, the raw output of a capture is **not** committed: the
two sets are ~168 MB across 88 files, which is not something to put in the history
of a repository this size for the sake of a review that ends in their deletion.

Object key order is sorted on write, since it carries no meaning. **List order is
not** — record ordering is part of the contract, and `/exposures` deliberately
changed its sort to `(day_obs, seq_num)`, so that is a difference worth seeing
rather than hiding.


---

## 2. The authentication model changed

This is the single change in the refactor most likely to be missed by reading
diffs, and the one with the widest blast radius. It was flagged in the original
plan as an open question requiring sign-off before implementation began; the
resolution is recorded here because it is recorded nowhere else in the tree.

### Before

Every data endpoint took the caller's token as a FastAPI dependency and threaded it
down to the upstream call:

```python
@app.get("/exposures")
async def read_exposures(..., auth_token: str = Depends(rsp_auth)):
    exposures = get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
```

`rsp_auth`, `jira_auth`, and `zephyr_auth` were module-level dependency instances in
`web_app/main.py`. Each upstream request was made with the token of the user who
made the request, resolved from the `Authorization` header or, failing that, from
the environment.

### After

No endpoint takes a token. Adapters resolve their own credentials at request time
from the source named by their `auth_source` class attribute:

```python
class RestClient:
    auth_source = "rsp"

    def _get_token(self) -> str:
        return retrieve_access_token(AUTH_SOURCES[self.auth_source])
```

`JiraApiMixin` overrides `auth_source = "jira"`, `ZephyrAdapter` sets
`auth_source = "zephyr"`, and `RubinNightsClientsMixin` resolves the RSP token once
per process for the `rubin_nights` client bundle.

`retrieve_access_token` was reduced to what it already actually did: read the environment
variable named by the source, or fail. Its RSP notebook-discovery branches and its
request-header fallback are gone, along with the `use_rsp_utils` config flag that
gated the former and the `request` parameter that fed the latter. The next
subsection explains why removing them changed nothing.

### The old model was already a service-account model in practice

Worth being clear about what was actually lost, because it is less than the diff
suggests: **the `Depends()`-injected token never resolved a user credential.**

`retrieve_access_token` used to try four sources in a fixed order, and the request
header was the *last* of them:

1. RSP notebook discovery (`RSPDiscovery.get_token`), for the `rsp` source only
2. `lsst.rsp.utils.get_access_token()`, same
3. **the environment variable named by the source's config**
4. the request's `Authorization` header

Step 3 has to be populated for the application to work at all — it is how the
deployment supplies credentials, and it is the only source available to any code
path without a request in scope. So in every real deployment step 3 returned a
token and step 4 was never reached. The header fallback could only ever fire on a
machine where `ACCESS_TOKEN` (or `JIRA_API_TOKEN`, or `ZEPHYR_API_TOKEN`) was
unset, which is a machine where most of the application does not function.

The endpoint-level `Depends(rsp_auth)` was therefore decorative: it threaded a
parameter through every signature to deliver a value that came from the
environment regardless. Removing it changed the plumbing, not the credential.

### So the resolution chain was collapsed to match

Steps 1, 2 and 4 have all been removed. Steps 1 and 2 existed to support running
this code inside an RSP notebook, which the service no longer does; step 4 was
unreachable, as above. `retrieve_access_token(config)` now reads one environment
variable, and the `use_rsp_utils` flag and `request` parameter that existed only to
drive the removed branches are gone with them.

**A missing token is now a 500, not a 401.** Callers do not authenticate against
this service, so a token that will not resolve is a misconfigured deployment rather
than a bad request, and saying `401` invited exactly the wrong diagnosis. The
response detail is the generic `"Server configuration error"`; the variable that is
actually unset is named in the log instead — the same
log-the-cause-return-a-generic-detail rule the service layer follows
([§7](#7-error-handling-and-the-frontend-contract)).

### What reviewers should check

- **User authentication is now entirely an upstream concern.** The application does
  not authenticate its callers. Access control is enforced by the RSP gateway for
  the internal deployment and by nginx/ingress for the public one. Nothing in this
  repository enforces it, and nothing in this repository will notice if that
  enforcement is misconfigured.
- **The deployment must provide service-account credentials** in the environment
  (`ACCESS_TOKEN`, `JIRA_API_TOKEN`, `ZEPHYR_API_TOKEN`) for both the API container
  and the refresh-worker container. The worker has no request context at all, so it
  has no other source of credentials.
- **Upstream audit logs attribute every query to the service account.** This is not
  new — see above; they already did.
- `utils/auth.py::get_access_token`, the FastAPI dependency factory that
  implemented the old model, has been removed along with the tests that only
  existed to exercise it. `retrieve_access_token`, `get_auth_header`,
  `get_jira_hostname`, `AUTH_SOURCES` and `Server` all remain and are all still
  used.

---

## 3. Logic that changed because of caching

These changes share one cause. A cache entry is keyed by dayobs and is reused
across every request that touches that night, so **anything whose value depends on
the requested range cannot be stored in it.** Each endpoint that had such a value
had to resolve it differently, and none of these changes are visible as an
intentional behavioural edit in the diff — they look like incidental refactoring.

### `/context-feed` — `timestampProcessEnd` is recomputed on read

`get_consolidated_messages` synthesises "Task Change" rows whose
`timestampProcessEnd` is bounded by the query window it was given. Cached per
night, each entry would bake in an end timestamp derived from that night's fetch
window rather than from the range the user actually asked for — so a 7-day request
assembled from 7 single-night entries would show 7 truncated task changes instead
of one continuous sequence.

`RubinNightsContextAdapter` documents the field as unreliable, and
`ContextFeedService.collate_response` recomputes it across the assembled range:
each task-change row's `timestampProcessEnd` becomes the `time` of the next
task-change row, or of the final record for the last one. The wire format is
unchanged; the values are now correct for the requested range rather than for the
cache granularity.

### `/obs-status` — the carry-in event is fetched a whole day earlier

Observatory status is a sequence of state *changes*, so interpreting a range
requires knowing the state it started in — the last event before the range begins.

The old code queried twelve hours earlier than the requested start and kept the
final event before the boundary:

```python
query_start_time = dayobs_start_time - TimeDelta(0.5, format="jd")
...
prior_events = obs_status_messages[obs_status_messages.index < dayobs_start_time_dt]
if not prior_events.empty:
    obs_status_messages = obs_status_messages.loc[prior_events.index[-1]:]
```

A twelve-hour lookback is not expressible as a dayobs cache key. The adapter now
buckets every event into the dayobs of its own timestamp, and `ObsStatusService`
fetches `[start - 1, end]` and takes the *last event of the leading day* as the
carry-in:

```python
carry_in_day, *range_days = days
entries = [data[carry_in_day][-1]] if data[carry_in_day] else []
```

Same intent, wider lookback. A night whose last preceding state change was more
than 24 hours earlier still has no carry-in event, exactly as before.

### `/jira-tickets` — `isNew` moved from the adapter to the service

`isNew` is true when a ticket was created inside the requested window. That is a
property of the request, not of the ticket, so caching it would poison the entry
for every other range.

The adapter now emits a `created_utc` field instead; `JiraTicketsService` computes
`isNew` from it and `pop`s `created_utc` before responding. The wire format is
unchanged — `created_utc` never reaches the frontend.

The adapter also buckets a ticket into *both* the dayobs it was created in and the
dayobs it was last updated in (matching the upstream JQL, which is an `OR` over the
two). A ticket can therefore appear in up to two cache entries; the service
deduplicates by key at collation, keeping the first occurrence in dayobs order.

### `/exposures` and `/data-log` share one cache entry

`ConsdbExposuresAdapter` caches `SELECT e.*, q.*` — the full exposure record joined
to quicklook and, where available, to the transformed EFD. `DataLogService` returns
it whole; `ExposuresService` projects the curated `EXPOSURE_COLUMNS` list on read.

This is a **documented trade-off**: the cache holds more columns than either
endpoint needs, in exchange for one upstream query and one refresh cycle serving
both endpoints instead of two of each. Neither endpoint's response shape changed.

### Visit maps cache the un-augmented visit record

`ConsdbVisitsAdapter` caches the raw `visit1 ⋈ visit1_quicklook` rows. The
`rubin_nights` augmentation that the old `get_visits(..., augment=True)` applied is
pure local computation, so it now runs on read in `VisitMapsService` rather than
before the cache write. Both `/multi-night-visit-maps` and `/static-visit-map`
therefore derive from a single cache entry — the old code called `get_visits` with
`augment=True` for one and `augment=False` for the other, which would have needed
two entries.

Note the consequence: a hot cache still pays the figure and PNG build cost on every
request. Only the data fetch is cached, not the rendering.

### Time accounting was split across the cache boundary

`get_time_accounting` did everything in one function: augment the visits, run the
kinematic slew model against the EFD, then reduce over the twilight windows. The
expensive part is the slew model; the range-dependent part is the reduction (the
filter-change split has to be computed across night boundaries, so it cannot be
done per-night and summed).

The split follows that seam. `VisitOverheadAdapter` caches per-visit `overhead` and
`visit_gap` rows per instrument and night; `ExposuresService._time_accounting`
performs the twilight-windowed reduction over the whole assembled range. The
adapter reads its visits from `ConsdbExposuresAdapter` rather than issuing its own
ConsDB query, so a refresh cycle warms it from the freshly-refreshed exposures —
which is why the refresh worker registers the two adapters in that order.

---

## 4. Structure and layout

### `web_app` is gone from the import path

The `web_app/` subpackage existed to separate the API from the notebook-era library
modules underneath it. With those modules deleted, the nesting no longer earned its
keep. Every module moved up one level: `lsst.ts.logging_and_reporting.main`,
`.services`, `.adapters`, `.middleware`, `.cache_ttl`, `.redis_client`,
`.refresh_worker`.

There is no "flatten" commit. The new code is born at the root and the old package
is deleted wholesale, because the two never contend for the same import path. Two
references outside the package changed: the uvicorn entrypoint string in
`run_logging_and_reporting.py`, and a stale `.gitignore` path
(`web_app/data/*` → `data/*`).

`web_app/main.py → main.py` is 27% similar and will not be rename-detected. Most
old-service to new-service pairs are the same.

### `utils.py` became a package

The 696-line grab-bag split by concern:

| Module | Contents |
|---|---|
| `utils/dayobs.py` | dayobs and date conversions, ranges, contiguous runs |
| `utils/auth.py` | tokens, headers, `AUTH_SOURCES`, `Server` |
| `utils/serialization.py` | `make_json_safe`, `stringify_special_floats` |
| `utils/collation.py` | `flatten_sorted` |

`utils/__init__.py` was a transitional re-export shim during the migration so that
remaining `import utils as ut` callers kept resolving. It is now a package marker
with a docstring and nothing else — import from the concern module directly.

`current_dayobs_utc` was renamed `dayobs_at` (it takes a timestamp), and a
zero-argument `current_dayobs()` was added for the common case.

### One file per unit

`exposurelog_service.py` became two services. The ConsDB and Jira adapters are one
file each rather than one file per upstream. `scheduler_service.py` became
`services/visit_maps.py`, `services/static_visit_map.py`, and
`adapters/expected_exposures.py`.

Tests follow the same shape — `tests/adapters/`, `tests/services/`, `tests/utils/`,
one module per unit — with the base classes, middleware, worker and endpoint
contracts at the root of `tests/`.

`tests/test_api_endpoints.py` (2275 lines) was replaced by
`tests/test_endpoint_contracts.py` (317 lines). The old file tested endpoint
behaviour end to end with heavy mocking; the new one tests only what an endpoint
function is actually responsible for — parameter parsing, forwarding to
`service.handle_request`, pass-through of the result, and status mapping. Response
payloads are covered in `tests/services`, upstream parsing and caching in
`tests/adapters`.

---

## 5. Function moves git will not show you

Everything in this section moved between files that are not rename-related, so the
diff shows a deletion and an unrelated addition. Where the code changed in transit,
the change is described.

### `rubin_nights_service.py` (1210 lines) fanned out to five destinations

| Function(s) | New home | Changed? |
|---|---|---|
| `decode_states`, `is_unknown`, `contains_daytime`, `contains_operational`, `contains_fault`, `contains_weather`, `contains_downtime`, `contains_idle`, `counts_as_fault_loss` | `services/obs_status.py` | Logic identical. Every docstring condensed from a full numpydoc block to one or two lines. |
| `get_obs_status_intervals`, `build_ms_dayobs_intervals`, `build_ms_night_intervals`, `sum_interval_overlap`, `get_availability` | `services/obs_status.py` | Logic identical. Docstrings condensed; step-by-step inline comments removed. `build_ms_night_intervals` became a comprehension. |
| `_twilight_windows_by_dayobs`, `_obs_start_tai_to_utc_ms`, `_compute_filter_changed`, `_sum_on_sky_within_twilight` | `services/exposures.py` | Logic identical, docstrings condensed. |
| `_compute_closed_hours` | `services/exposures.py` | **Signature changed** — see below. |
| `dayobs_to_unix_ms`, `almanac_to_unix_ms` | `utils/dayobs.py` | Unchanged. |
| `dayobs_to_noon_utc` | — | Deleted; callers use `get_utc_datetime_from_dayobs_str` with `astropy.Time`. |
| `get_visits`, `get_open_close_dome`, `get_context_feed`, `get_obs_status_events`, `get_time_accounting` | adapters | Dissolved — see [§3](#3-logic-that-changed-because-of-caching) and [§6](#6-data-source-changes). |

**`_compute_closed_hours` changed shape.** It took a `pandas.Series` and resolved
the day under test with `row.get("day_obs", row.name)` — tolerating the value
arriving either as a column or as the index name, depending on whether the caller
passed a raw or a grouped frame. It now takes a plain `dict` and requires an
explicit `day_obs` key, which the new `_aggregate_dome_hours` supplies. Same
arithmetic, but a caller that relied on the index-name fallback would now raise.

**`sum_interval_overlap` gained a documented precondition.** Its new docstring
states that both interval lists must be time-ordered. That was always true of the
callers and the algorithm always depended on it; it had simply never been written
down.

### Other cross-file moves

| From | To | Changed? |
|---|---|---|
| `jira.py::get_system_names` | `adapters/mixins.py::JiraApiMixin.get_system_names` | Body unchanged, now a `@staticmethod` on a mixin; docstring rewritten. |
| `source_adapters.py::NarrativelogAdapter.add_instrument` | `adapters/narrativelog.py::NarrativelogCachedAdapter._add_instrument` | Per-message instead of per-list; see below. |
| `utils.py::build_block_response` | `services/block_details.py::BlockDetailsService.collate_response` | Fixes a latent bug; see below. |
| `web_app/main.py::_encode_png_payload` | `services/static_visit_map.py` | Verbatim. |
| `web_app/services/almanac_service.py::_as_utc_datetime`, `_compute_elapsed_twilight_hours` | `services/almanac.py` | Verbatim. |
| `scheduler_service.py::_add_dec_labels`, `_add_ra_labels`, `_add_graticules`, `_style_text`, `_style_axes`, `_style_figure`, `_compute_nvisits_bundle`, `build_static_visit_map` | `services/static_visit_map.py` | Verbatim. |
| `scheduler_service.py::_prepare_visit_maps_data`, `_get_visit_map_config` | `services/visit_maps.py` | Verbatim. |
| `scheduler_service.py::build_visit_maps_using_builder` | `services/visit_maps.py` | One change: `°` escapes in the Bokeh tooltip HTML became literal `°`. |
| `exposure_log.py::ExposurelogAdapter.get_messages` (the `"none"` → `"unknown"` flag remap) | `adapters/exposurelog.py::ExposurelogCachedAdapter._fetch_run` | Same behaviour, different layer. |

### Two Jira filters became one

`filter_tickets_with_instrument_match` and
`filter_tickets_without_instrument_match` were identical apart from a boolean
polarity flip. They are now `filter_tickets_by_instrument(tickets, instrument,
exclude=False)`.

Both old docstrings claimed the function "adds a `'url'` field to the ticket". It
never did — the URL is set by the adapter when the record is built. The new
docstring drops the claim.

### `_add_instrument` lost a warning and gained a corrected constant

The old `add_instrument` raised `UnknownTelescopeWarning` via `warnings.warn` for
any telescope outside `{AuxTel, MainTel, Simonyi}`. That warning class lived in
`exceptions.py`, which is deleted ([§11](#11-deletions)); the unknown case now
silently maps to `instrument = None`, as it always did for the record itself.

The magic date was `LSST_DAY = 20250120`, while the docstring above it said
"Prior to 2025-01-19 we assume Instrument=LSSTComCam". The constant is now
`LSSTCAM_FIRST_DAY = 20250120` with a docstring that matches it.

### `build_block_response` fixed an import-time bug on the way

The old BLOCK URLs were module-level f-strings in `utils.py`:

```python
ZEPHYR_BLOCK_BASE_URL = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/projects/BLOCK?..."
JIRA_BLOCK_BASE_URL = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/browse/"
```

Evaluated at import. If `JIRA_API_HOSTNAME` was unset — or set after import — every
BLOCK URL served for the life of the process was `https://None/browse/BLOCK-42`,
silently.

`BlockDetailsService.collate_response` now calls `get_jira_hostname()` per
response, which raises a 500 with `"Jira hostname not configured"` when the
variable is missing. A misconfiguration that used to produce plausible-looking
broken links now fails loudly.

### `Almanac` was reimplemented, not moved

The 192-line `Almanac` class (a `SourceAdapter` subclass) is now
`AlmanacCachedAdapter._compute_night`, about 30 lines. Same astroplan calls,
same field names, same values, same UTC-ISO-truncated-to-seconds formatting.

Two differences worth knowing:

- The old `get_almanac` constructed a whole `Almanac` object per night inside a
  loop, each one recomputing an `EarthLocation.of_site` lookup and a full set of
  sun and moon events. The observer is now built once per process
  (`functools.cached_property`) and only the per-night events are computed.
- `Almanac.as_dict` returned a **two-element tuple**: the data dict and a
  `help_dict` of human-readable annotations (`"Morning Nautical Twilight":
  "(-12 degrees)"`, `"Moon Illumination": "(% illuminated)"`). The old
  `get_almanac` discarded the second element and never surfaced it, so no endpoint
  ever returned it — but if anything downstream was reconstructing those
  annotations from a copy of this table, the source of truth is now gone. The
  discarded `dataframe`, `events`, and `as_records` accessors were likewise unused.

---

## 6. Data-source changes

### `/data-log` and `/exposures` no longer go through `rubin_nights`

`ConsdbExposuresAdapter` issues SQL directly to `/consdb/query` via `SqlClient`.

The old `/data-log` path issued two queries — `ConsdbAdapter.get_exposures` and
`ConsdbAdapter.get_transformed_efd_data` — and merged them with
`pd.merge(..., on="exposure_id", how="left")` in Python. That is now a single SQL
`LEFT JOIN`.

The EFD join is **deployment-conditional**. `efd_<instrument>.exposure_efd` does not
exist at the summit or base deployments, so `efd_transform_available()` checks the
deployment identity and omits the join there. An unset or unrecognised
`EXTERNAL_INSTANCE_URL` defaults to *available*, which is what dev and test want.

### Visit maps query ConsDB directly too

`get_visits` used `clients["consdb"].get_visits(instrument, t_start, t_end,
augment=...)` from `rubin_nights`. `ConsdbVisitsAdapter` now issues
`SELECT v.*, q.* FROM cdb_<instrument>.visit1 v LEFT JOIN
cdb_<instrument>.visit1_quicklook q` itself. See
[§3](#3-logic-that-changed-because-of-caching) for why it is cached un-augmented.

### The ConsDB SQL is string-interpolated, so the inputs are validated

Instrument and dayobs are interpolated into raw SQL. `ConsdbSqlMixin.fetch`
therefore rejects them before they reach the query: an instrument outside
`INSTRUMENTS` raises 422 `Unknown instrument: 'x'`, and a dayobs that does not parse
as `%Y%m%d` raises 422 `Invalid dayobs: N`. This is the load-bearing reason those
validations exist, not a general input-hygiene pass.

### Duplicate column names from the quicklook join

`SELECT e.*, q.*` yields duplicate column names where the two tables overlap.
`ConsdbSqlMixin._rows_from_result` overrides the plain zip in `SqlClient` and keeps
the **first non-null** value for a duplicated column, so a null on the join side
never clobbers a valid primary value. Merged column names are logged at debug.

### `rubin_nights` is still used, for three things

The EFD-backed adapters (`rubin_nights_dome`, `rubin_nights_obs_status`,
`rubin_nights_context`) reach it through `RubinNightsClientsMixin`, which builds
the `get_clients()` bundle once per process. `VisitOverheadAdapter` uses
`rubin_nights.augment_visits` and `rubin_nights.rubin_scheduler_addons`. The visit
map services use `augment_visits` on read.

`rubin_nights.dayobs_utils` is no longer used at all — dayobs conversion is local
to `utils/dayobs.py`.

### Zephyr dropped the `ts-planning-tool` dependency

`ZephyrAdapter` calls the Zephyr Scale REST API (`/testcases/{key}`) directly
instead of going through `lsst.ts.planning.tool.ZephyrInterface`.

**`ts-planning-tool =0.1` was dropped from `conda/meta.yaml`** along with its last
importer. Nothing else in the tree imports `lsst.ts.planning`. If anything outside
this repository relied on that package being present in the environment as a
side effect of this one depending on it, it will need to declare it itself.

Error handling changed with the rewrite. The old code caught every exception per
key and skipped the key with a warning, so an auth failure or an outage was
indistinguishable from a genuinely absent test case. The new adapter caches `None`
for a 404 (so an unknown key does not re-query upstream on every request) and
re-raises anything else.

---

## 7. Error handling and the frontend contract

### Errors are now uniform and comprehensively covered

Every endpoint's error translation lives in one place:

```python
def handle_request(self, *args, **kwargs) -> dict:
    try:
        return self.handle(*args, **kwargs)
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream failure in {name}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error in {name}") from e
```

Previously each endpoint hand-rolled its own `try`/`except` with inconsistent
coverage: some caught `ConsdbQueryError` and mapped it to 502, some caught
`BaseLogrepError`, most caught bare `Exception` and returned 500, and two swallowed
errors entirely.

### Exception detail no longer reaches the client

Old responses were `HTTPException(status_code=500, detail=str(e))` — raw Python
exception text on the wire, including in some cases the SQL that failed. The detail
is now generic and names only the service (`"Internal error in ExposuresService"`).
The full exception, with traceback, still goes to `logger.exception`.

The frontend never surfaced `detail` to users, so no user-visible message changes.
Anything parsing `detail` programmatically will need updating.

### Two endpoints stopped swallowing errors

`get_context_feed` returned `[]` and `get_obs_status` returned `{}` on any
exception. A failed upstream call was indistinguishable from a genuinely empty
night — the frontend rendered "no data" for both. Both now propagate to a 500 or
502.

### New 422s

| Source | Condition | Detail |
|---|---|---|
| `DayobsValidationMiddleware` | `dayObsStart > dayObsEnd` | `dayObsStart (N) must not be after dayObsEnd (M)` |
| `DayobsValidationMiddleware` | malformed dayobs | `Invalid dayObsStart: N` |
| `ConsdbSqlMixin` | unknown instrument | `Unknown instrument: 'x'` |
| `ConsdbSqlMixin` | malformed dayobs | `Invalid dayobs: N` |

### An unresolvable upstream token is a 500, not a 401

Any endpoint whose adapter cannot resolve its service-account token now fails with
`500 Server configuration error`. It used to be
`401 <label> authentication token could not be retrieved by any method.`

The 401 made sense when the token could have come from the caller's own
`Authorization` header — it read as "your credentials are missing". Nothing about
the request can influence it any more, so it is a deployment fault and is reported
as one. See [§2](#2-the-authentication-model-changed) for the full change.

Worth knowing for triage: a deployment that forgets `JIRA_API_TOKEN` will now show
up as a 500 on `/jira-tickets` and `/block-details` rather than a 401, and the
env var that is actually missing appears only in the backend log.

### `/expected-exposures` returns 404 for an empty result

When `rubin_sim.sim_archive` finds no matching simulation for a night it raises
`NoMatchingSimulationsFoundError`, which `ExpectedExposuresService` maps to a 404.

This is **inconsistent with every other endpoint**, which returns an empty container
and a zero count when there is no data for the requested range. It is a known
inconsistency, carried forward deliberately rather than changed under cover of a
refactor; there is no ticket for it yet. Previously the same condition produced a
500, so the status changed but the "this is not a success" signal did not.

### `/context-feed` returns a fixed column list

`cols` was whatever `get_consolidated_messages` happened to return for that call.
It is now the module constant `CONTEXT_FEED_COLS` — the same twelve display columns
the adapter filters the frame down to before caching. If the upstream shortlist
changes, this list must be changed with it.

### `/data-log` record order is unspecified; `/exposures` is now guaranteed

The two endpoints read the same cache entry, and only one of them sorts it.

`ExposuresService.collate_response` orders explicitly — nights ascending, and
within a night by `seq_num`:

```python
for dayobs in sorted(data):
    records.extend(sorted(data[dayobs], key=lambda record: record.get("seq_num") or 0))
```

`DataLogService.collate_response` sorts only the dayobs keys, so rows within a
night keep whatever order they arrived in:

```python
records = [record for dayobs in sorted(data) for record in data[dayobs]]
```

Neither the old nor the new ConsDB query has an `ORDER BY`, and neither path has
ever sorted in Python, so `/data-log`'s within-night order has always been
whatever Postgres returned. In practice that has probably always *looked*
time-ordered: `exposure` is append-only, rows go in in time order, and a plain
sequential scan tends to return them in physical order. That is a property of the
query plan, not a contract.

What is new is that the plan changed. The old query was a single-table scan; the
new one is `exposure LEFT JOIN visit1_quicklook` plus, where available, a
transformed-EFD join. A hash or merge join can emit rows in an order unrelated to
the driving table's physical order, where a sequential scan cannot.

Anyone relying on `/data-log` arriving in time order should not. The frontend
sorts this data by `seq_num` so this does not present an actual issue.

### The dayObs range convention is inconsistent, and this refactor makes it visible

The frontend always sends an inclusive range. The backend does not treat it
consistently, and the adapters are the only layer where each upstream's own
convention is normalised.

| Endpoint | `dayObsEnd` is treated as |
|---|---|
| `/exposures`, `/data-log`, `/jira-tickets`, `/narrative-log`, `/exposure-flags`, `/exposure-entries`, `/night-reports`, `/multi-night-visit-maps`, `/static-visit-map` | **exclusive** (service subtracts a day) |
| `/context-feed`, `/expected-exposures` | **inclusive** |
| `/obs-status` | **inclusive**, and expanded by one day in each direction internally |
| `/almanac` | **inclusive**, with the *start* shifted forward a day (records are keyed by morning twilight) |

This was not fixed here. Unifying it changes the contract with the frontend and
belongs in its own change with its own testing. **A follow-up ticket will be
raised; there is no ticket number yet.**

### Smaller contract changes

- **All data endpoints are now sync `def`.** FastAPI runs them in its threadpool.
  They were `async def` with blocking calls inside, which is worse. The explicit
  `run_in_threadpool` wrapping around visit-map generation is gone as a result.
  Only `/health` and `/version` remain `async`.
- **Handler functions were renamed to a `read_*` convention**
  (`read_nightreport` → `read_night_reports`, `multi_night_visit_maps` →
  `read_multi_night_visit_maps`, `static_visit_map` → `read_static_visit_map`).
  Route paths are unchanged, but the generated OpenAPI `operationId`s are not.
- **`/mock-exposures` was deleted.** It read `data/exposures-lsstcam0413.ecsv` via a
  relative path that could not have resolved inside a deployed container.
- **`Service.handle` and `Service.handle_request` swapped roles mid-stack.** In
  early commits `handle_request` was the abstract method subclasses implemented and
  `handle` was the error-translating wrapper. This is worth knowing when reading
  the reconstructed history: the names mean the opposite of what they did in the
  first two-thirds of the branch.

---

## 8. Middleware

### `CacheControlMiddleware` moved and gained a tier

It arrived on `develop` separately (OSW-2415) and was relocated from
`web_app/middleware/` to `middleware/` and reworked:

- Two TTL tiers became three: `TODAY_TTL`, `MUTABLE_TTL`, `HISTORIC_TTL`, all
  centralised in `cache_ttl.py` rather than defined inline.
- Error responses get `Cache-Control: no-store`. Previously a 500 could be cached
  by the proxy and the browser for the full historic max-age.
- Mutable endpoints get `MUTABLE_TTL` even when the request carries no dayobs
  parameters, which is what covers `/block-details` (keyed by BLOCK id, not by
  date).
- `_today_dayobs()` was replaced by the shared `utils.dayobs.current_dayobs`.
- The "There's an error getting start/end dayobs" `logger.error` became a warning
  that names the path and the offending values.

### `DayobsValidationMiddleware` is new

It rejects malformed and inverted ranges before they reach a handler. It only
applies to paths that match a real route, so a request to a nonexistent path with
a bad dayobs still gets a 404 rather than a 422.

### Two planned middlewares were not built

`ErrorHandlingMiddleware` was dropped — `Service.handle_request` covers the same
ground, and the `BaseLogrepError` hierarchy it was specified to serialise has been
deleted. `PublicAccessMiddleware` is deferred with the public-facing release.

---

## 9. Caching, configuration and operations

### The refresh worker is a separate process

It was to be a daemon thread inside the API process. It is a separate container
with its own console-script entrypoint (`run_refresh_worker`), its own
`docker/refresh_worker.sh`, and its own service in the dev compose stack.

**Exactly one instance must run per deployment.** Duplicate refreshes are harmless
in themselves — fetch-then-overwrite is idempotent — but they waste upstream calls.
Nothing in the code enforces this; it is a property of the deployment (one
container in compose, a single-replica deployment in Kubernetes). See
[§14](#14-divergence-from-the-original-refactor-plan), item 11, for what was
originally specified instead.

Registration order in `run_refresh_worker.py` matters: `visit_overhead` is listed
after `consdb_exposures` because it reads that adapter's cache.

The almanac adapter is deliberately **not** registered — its data is computed
locally and never changes, so it takes the historic TTL even for today.
`IdCachedAdapter` subclasses (`zephyr`, `jira_block`) are not registered either;
they have no "today" entry to refresh.

### New environment variables

| Variable | Effect |
|---|---|
| `ND_CACHING_DISABLE_REDIS` | Replaces the Redis client with `DisabledRedis` — every read misses, every write is dropped, every lock is won. The refresh worker logs a warning and exits immediately, since it would have nothing to warm. |
| `ND_CACHING_DISABLE_NGINX` | *(frontend repo)* Makes nginx bypass the proxy cache so all requests hit the backend directly. `docker/nginx.conf` became `nginx.conf.template` to support this. |
| `LOG_LEVEL` | Drives `logging.basicConfig` in both entrypoints. Defaults to `INFO`. |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` | Redis connection. Default to `localhost:6379` db 0; the dev compose stack sets `REDIS_HOST=redis`. |

### The auth environment variables are now mandatory, with no fallback

`ACCESS_TOKEN`, `JIRA_API_TOKEN` and `ZEPHYR_API_TOKEN` are not new, but they used
to sit at the end of a chain that could also resolve a token from an RSP notebook
or from the caller's `Authorization` header. That chain is gone
([§2](#2-the-authentication-model-changed)): the variable is now the only source,
and an unset one fails the request with a 500.

Both containers need them — the API and the refresh worker. The worker has no
request context and no notebook to fall back on, so a worker deployed without them
will log a token error on every adapter, every cycle, and warm nothing. `JIRA_API_HOSTNAME`
is required alongside them for the Jira and Zephyr BLOCK URLs.

The two disable switches exist for debugging: when a value looks wrong, you need to
be able to tell whether the backend is producing it or a cache is replaying it.
Unset, empty, and `"0"` all leave caching on; any other value turns it off.

### TTLs are two-level and they stack

`cache_ttl.py` defines a client `max-age` and a Redis TTL for each of the three
data kinds:

| Kind | Client `max-age` | Redis TTL |
|---|---|---|
| Historic | 1 day | 30 days |
| Today | 5 min | 15 min |
| Mutable (past dayobs) | 5 min | 1 hour |

The two stack: a response built from a nearly-expired Redis entry can then sit in a
browser cache for its full `max-age`. Only the Redis copy can be flushed by hand,
which is why the client TTLs are kept short relative to their Redis counterparts.

`TODAY_TTL` doubles as the refresh worker's default interval, so a client is never
served data staler than one refresh cycle. `TODAY_TTL_REDIS` must comfortably
exceed it so today's entry cannot expire between cycles.

### Non-finite floats are rejected at the cache boundary

`json.dumps` defaults to `allow_nan=True` and will happily emit the non-standard
`NaN` / `Infinity` / `-Infinity` tokens. Those round-trip through Python's own
`json.loads` but are invalid JSON, so anything else reading the cache directly
would break on them.

`_store` passes `allow_nan=False`, so a non-finite value that escapes an adapter's
sanitisation raises at write time instead of writing spec-invalid JSON into Redis.
It also passes `ensure_ascii=False` and compact separators — nothing reads this
JSON by eye, and log and comment text stores as UTF-8 rather than as `\uXXXX`
escapes.

### Redis must be configured as a cache

`maxmemory` with `allkeys-lru` eviction, persistence disabled. The dev
docker-compose stack (frontend repo) provides this; **production deployments must
match it.** Provisioning is external (Phalanx); the only repo-side change is the
`redis-py` dependency in `conda/meta.yaml`.

Redis is assumed always available — if the server is running, Redis is reachable,
and no fallback path is implemented. The five-second socket timeouts only bound how
long a request can hang if that assumption breaks.

---

## 10. Logging

### `getLogger("uvicorn.error")` is gone

Several modules did this:

```python
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)
```

That hijacks uvicorn's own logger and forces it to DEBUG globally, at import time,
for the whole process. Every module now uses `logging.getLogger(__name__)`, and
level is set once per entrypoint from `LOG_LEVEL`.

### `print` is gone

Roughly twenty-five `print()` calls went with `source_adapters.py`, `consdb.py` and
`efd.py`, along with the `verbose` flag plumbing that gated them. Anything worth
keeping is now `logger.debug`.

### The refresh worker reports on itself

Each cycle logs its duration and a success/failure count per adapter, and warns
when a cycle consumes more than half its interval — the signal that the worker is
about to fall behind.

---

## 11. Deletions

### Superseded — replaced by adapter/service equivalents

`web_app/main.py`, all ten `web_app/services/*.py`, `almanac.py`, `consdb.py`,
`exposure_log.py`, `jira.py`, `source_adapters.py`.

Tests: `test_api_endpoints.py` (2275), `test_services.py` (1498),
`test_utils.py` (489), `test_jira.py` (324), `test_dome_hours.py` (293),
`test_adapters.py` (82), `test_almanac_service.py` (49).

Deleting `source_adapters.py` also removed the notebook-era `SourceAdapter` surface
that nothing in the API used: `day_table`, `analytics`, `check_endpoints`,
`keep_fields`, `get_status`, `urls`, `protected_get`, `protected_post`.

### Dead before this refactor — deleted because it was in the way

| Item | Evidence |
|---|---|
| `efd.py` (253 lines) | Zero importers at the merge base. Includes an `async def main()` whose own output says it "will take a long time - maybe 2 hours". |
| `utils.py::date_hr_min`, `fallback_parameters`, `DatetimeIter`, `hhmmss`, `Timer`, `dayobs_str` | No call sites anywhere in the tree. |
| `utils.py::tic`, `toc` | Only consumers were `efd.py` (itself dead) and a docstring. |
| `utils.py::datetime_to_dayobs` | Only consumer was `source_adapters.py`. |
| `tests/test_all_sources.py` | Tested the deleted adapters. |
| `/mock-exposures` and `get_mock_exposures` | Read a file by relative path that could not resolve in a container. |
| `utils/auth.py::get_access_token` | The FastAPI dependency factory for the old per-request-token model. No production caller after the `rsp_auth` purge. |
| `retrieve_access_token`'s RSP notebook branches, `Authorization`-header fallback, `use_rsp_utils` flag and `request` parameter | Supported running inside an RSP notebook, which the service does not do, and a header path the environment variable always pre-empted. See [§2](#2-the-authentication-model-changed). |

### `exceptions.py` was deleted entirely

`BaseLogrepError` and its seven subclasses — `StatusError`, `ConsdbQueryError`,
`ConsdbQueryWarning`, `NoRecordsWarning`, `UnknownTelescopeWarning`,
`NotAvailWarning`, `ExcludeInstWarning` — are gone. Errors are now standard
`requests` exceptions at the adapter layer and `fastapi.HTTPException` at the
service layer.

Anything catching these by name, in this repository or outside it, will no longer
compile. The one behavioural consequence inside the repository is the lost
`UnknownTelescopeWarning` in narrative-log instrument mapping
([§5](#5-function-moves-git-will-not-show-you)).

### Still to be deleted before merge

The `review/` directory, `scripts/perf_test.py` and
`scripts/capture_endpoints.py`. Everything in `review/` is refactor-only
scaffolding:

```
review/BACKEND_REFACTOR_PLAN.md           the plan as it ended
review/BACKEND_REFACTOR_PLAN_ORIGINAL.md  the plan as first written
review/REFACTOR_ODDITIES.md               this document
review/capture-compare.md                 endpoint output parity, before vs after
review/performance/BEFORE.json            performance run, pre-refactor
review/performance/AFTER.json             performance run, post-refactor
review/performance/COMPARE.md             the two joined into a table
```

and the two scripts are the harnesses that produced the evidence. A single final
commit removes the lot and does nothing else, so it can be taken after review,
immediately before the epic merges.

---

## 12. Serialisation fixes made in passing

`utils/serialization.py` acquired four small behavioural fixes while being moved.
They are behavioural, they are easy to miss inside a file move, and they affect
every endpoint that returns floats or timestamps.

- **`make_json_safe` always returns a list for tuple input.** It previously
  preserved tuples. JSON has no tuple type, so a stored tuple always came back as a
  list on a cache hit anyway — the distinction was never preserved end to end.
- **`make_json_safe` dropped a dead `pd.isnull()` check** inside the `pd.Timestamp`
  branch. `pd.Timestamp("NaT") is pd.NaT`, so a null Timestamp is always caught by
  the earlier identity check.
- **`make_json_safe` guards `np.timedelta64` the way it already guarded
  `np.datetime64`.** `np.timedelta64("NaT")` now returns `None` instead of a raw
  `NaN` float — which, post-`allow_nan=False`, would have raised at cache write.
- **`stringify_special_floats` matches `np.floating`, not just builtin `float`.**
  `np.float64` subclasses `float` and was always caught; `np.float32` and friends
  were not, so NaN and Inf in non-float64 columns passed through unstringified.

---

## 13. Known gaps and deferred work

Things left imperfect on purpose. Each is a decision, not an oversight.

### Zephyr is still not parallel

`ZephyrAdapter._fetch_from_source` issues one GET per test-case key in a `for`
loop. This is not a regression — the old `get_test_cases` was `async` but `await`ed
inside a loop, which is equally sequential — but the `IdCachedAdapter` batch
contract now makes it *look* like it should be batched, and the cost is more
visible now that everything around it is cached. Ticket.

### The exposure log does not paginate

`ExposurelogCachedAdapter` makes a single request with `limit=2500`, inheriting the
old adapter's `# TODO: pool paginate`. Narrative log and night report both paginate
through `RestClient._get_json_paged`. A night with more than 2500 exposure-log
messages would silently truncate.

### Swagger/OpenAPI documentation

Most handlers declare no `response_model` and carry no docstring; only
`/exposures`, `/multi-night-visit-maps` and `/static-visit-map` have one. The
generated `/docs` is correspondingly thin. A documentation pass was scoped into the
plan and deferred out of it. Ticket.

### Data-log JSON-ification is inconsistent

`/data-log` and `/context-feed` run `stringify_special_floats`, rendering NaN and
Inf as the strings `"NaN"` and `"Infinity"`. `/exposures` does not, so the same
underlying value is a string on one endpoint and `null` on another. Separately,
values that are not JSON-representable cannot survive the cache round-trip at all.

Resolving this means deciding, per endpoint, whether a given field is *used* (and
so needs a number) or *displayed* (and so belongs to the frontend). Not attempted
here; no ticket yet.

### `/static-visit-map` returns wrong images under concurrency

Pre-existing, not introduced here, but the performance work surfaced it and it is
the most serious thing in this section. `build_static_visit_map` renders through
pyplot's global state machine, and FastAPI dispatches the sync handler into a
threadpool, so concurrent requests contend for one process-wide active-figure and
active-axes pointer. Rendering a fixed visits frame across ten threads and comparing
the PNG bytes against a single-threaded reference gives **92–98 of 100 renders
wrong**, against **0 of 100** when the render is serialised behind a lock. Roughly a
quarter of the failures raise `IndexError: list index out of range` from inside
healpy/MAF; the rest return a wrong image with a 200 and no error at all.

The silent-wrong case is the dangerous one: an observer can be served another
request's sky map with nothing to indicate it.

The obvious local fix does not work. The pyplot calls in `static_visit_map.py`
(`plt.figure`, `plt.sca` at line 117, `plt.close`) are not the whole story — tracing
the mutations shows MAF and healpy creating and switching figures themselves, and
`plt.sca` is load-bearing because `hp.graticule()` takes no axes argument and draws
on `plt.gca()`. Rewriting the service against matplotlib's object-oriented API is
therefore not possible without replacing those libraries. The options are a
module-level lock around the render (cheap; renders already serialise on the GIL, so
it costs throughput that was never there) paired with a bounded semaphore so queued
requests shed rather than occupy threadpool workers, or a pre-warmed process pool
(genuine parallelism, days of work). Caching the rendered PNG — see the next entry
but one — would make the lock rarely contended but does not fix correctness on its
own, since distinct ranges still race. Ticket.

### `build_static_visit_map` leaks a figure on every failed render

`plt.close(fig)` is the last statement of the function rather than a `finally`, so
any exception between the figure being made current and that line leaves the figure
registered in pyplot's global manager, where nothing collects it. Each leaked figure
holds a full healpix image.

The concurrency run above makes the arithmetic exact: 73 `plt.close` calls and 27
exceptions across 100 renders. Every render that raised leaked, every render that
completed did not. Under the burst conditions in the performance capture, where
62–67% of requests failed, most requests would leak — unbounded growth in a
long-running server.

Independent of the concurrency defect and worth fixing on its own: it is a
`try/finally`.

### Visit-map rendering is not cached

Only the data fetch is cached
([§3](#visit-maps-cache-the-un-augmented-visit-record)); augmentation, the Bokeh build
and `json_item` serialisation of a ~3.3 MB document run on every request. A fully hot
7-day `/multi-night-visit-maps` still takes **18.0 s** (from 36.9 s cold), where
`/static-visit-map` hot is 4.5 s and every non-map endpoint is under a second. Being
CPU-bound Python in a sync handler, that work serialises on the GIL, so ten concurrent
requests exhaust the 300 s timeout no matter how warm the cache is.

Deferred — this wants its own ticket, and it may be a regression of
[OSW-2187](https://rubinobs.atlassian.net/browse/OSW-2187). Three separable levers, in
rough order of cost:

- **Range-keyed document cache.** The rendered figure genuinely cannot join the
  per-day chunk scheme — a 7-day document is not the concatenation of seven one-day
  documents — but it can still be cached on
  `(start, end, instrument, applet_mode, theme)`. That gets no reuse across a rolling
  window, only across repeat views of the same range, which is the common frontend
  pattern. Needs a bounded TTL and eviction: ~3.3 MB per combination, without the
  per-day scheme's natural bound.
- **Cache the prepared frame per dayobs.** The *document* is not decomposable but the
  *preparation* is: `_prepare_visit_maps_data` and `rn_aug.augment_visits` are per-row
  transforms. `collate_response` currently concatenates all days and then augments the
  whole frame, which forces row work into range shape. Preparing per day and caching
  that fits `InstrumentDayobsCachedAdapter` as it stands and helps rolling windows too.
  Confirm first that the `NIGHT_STACKERS` entries are row-independent — a stacker
  computing night-relative quantities would make a per-day frame differ from a slice of
  the 7-day one — and note it costs a second entry per night, since the prepared frame
  is useless to `/static-visit-map`.
- **Take the render off the GIL.** Neither cache helps a burst of distinct ranges.
  That needs the build in a process pool, or a concurrency limit that sheds load
  instead of letting every request time out together.

Which of the first two is worth doing depends on how the 18 s splits between
preparation and Bokeh build/serialisation. That was never measured; timing the three
phases on one request would settle it, and if the build dominates then the per-day
prepared frame is not worth its second cache entry.


---

## 14. Divergence from the original refactor plan

The plan was first committed at 1054 lines and reached 1427 across 37 commits over
roughly two weeks, being amended after almost every slice. Both revisions ship for
this reason: `review/BACKEND_REFACTOR_PLAN_ORIGINAL.md` is the 1054-line original
and `review/BACKEND_REFACTOR_PLAN.md` the final state, so the delta this section
describes can be read directly rather than reconstructed from git. It shows where
the design changed under contact
with the code, and it is the reason several structures in the final tree have no
obvious motivation when read cold.

The plan is deleted before merge. This section preserves the delta.

### What the plan got right, and which held unchanged

These were specified up front and shipped as described:

- Caching lives entirely in the adapter layer; the service layer is a thin collator
  with no Redis interaction.
- Services are singletons injected via `Depends()`, one per endpoint.
- Per-key single-flight locks on cold misses, with waiters polling the cache.
- `refresh()` is fetch-then-overwrite, never delete-then-fetch, so a request
  arriving mid-refresh is served the previous value rather than falling into a
  cold-miss window.
- The immediate first refresh cycle on startup, and the finalisation pass that
  re-stores the previous dayobs with the historical TTL after 12:00 UTC rollover.
- Today's entry has a Redis TTL comfortably exceeding the refresh interval, so it
  cannot expire between cycles.
- Redis configured as a cache (`maxmemory`, `allkeys-lru`, persistence off) with
  one shared connection pool built at startup.
- `/exposures` degrades gracefully — dome and time-accounting failures surface as
  `open_dome_error` and `time_accounting_error` in the payload rather than failing
  the request.
- `/block-details` degrades the same way, hard-failing only when both sources fail.
- Each `Service` subclass declares its own typed signature; the base class does not
  take `**kwargs`.
- Partial cache hits: a 7-day request only fetches the days not already cached.
- Adapters are synchronous, for the reason the plan gave — the scientific Python
  stack underneath cannot be made async.

### Class model

1. **`BaseAdapter` was dropped.** The plan had `BaseAdapter` (ABC) with
   `CachedAdapter` extending it. There is one non-abstract `CachedAdapter` holding
   the cache loop, with three key-shape subclasses beneath it.
2. **`IdBasedAdapter` became `IdCachedAdapter`**, a sibling under `CachedAdapter`
   rather than a separate ABC, so it inherits the cache loop instead of
   reimplementing it.
3. **`InstrumentDayobsCachedAdapter` did not exist in the plan.** The plan assumed a
   universal `(adapter, dayobs)` cache key. ConsDB is per-instrument, so a composite
   `{instrument}:{dayobs}` key shape was needed.
4. **Transport was separated from caching.** `base_clients.py` (`RestClient`,
   `SqlClient`) is not in the plan. The plan instead specified an `adapters/http.py`
   holding `protected_get` / `protected_post` extracted from `source_adapters.py`;
   that file was never created and those helpers were deleted outright. A
   `RestCachedAdapter` was built during the work and then removed once the
   mixin-plus-cache-base composition proved cleaner.
5. **`Service.adapters: dict[str, CachedAdapter]` became named, typed constructor
   parameters.** The dict was untyped, made every access a string lookup, and gave
   no signal about which adapters a service actually required.
6. **`handle` and `handle_request` swapped roles.** The plan made `handle_request`
   the abstract method; it is now the concrete error-translating wrapper, and
   `handle` is what subclasses implement.
7. **The base class does contiguous-run batching.** The plan left it to each adapter
   to group cache misses with a `contiguous_runs()` helper. `_collate_runs` and
   `_partition_by_field` in the base class do it for them, and seed empty values for
   dayobs the upstream returned nothing for.
8. **Mutable data got its own TTL rather than reusing the short one.** The plan said
   mutable adapters "should return the short TTL for all dayobs". They now mix in
   `MutableDataMixin` and get a distinct `MUTABLE_TTL_REDIS` of one hour, rather
   than reusing today's fifteen minutes.
9. **`fetch_concurrently` is not in the plan.** Multi-adapter services fetched
   serially in the plan's design; `/exposures`, `/obs-status` and `/block-details`
   now fan their independent fetches out across a thread pool.

### Refresh worker

10. **It is a separate process, not a daemon thread inside the API.**
11. **The Redis leader lease was never implemented.** The plan made single-execution
    a code property: each cycle would acquire or renew a lease (`SET NX EX`, TTL
    2 × interval), only the leaseholder would refresh, a dead leader's lease would
    expire within about a cycle, and `stop()` would release it early. With the
    worker as its own single-replica deployment, that machinery was dropped —
    `RefreshWorker.__init__` does not take a Redis client at all. **Single execution
    is now an assumption about the deployment rather than something the code
    enforces.** Whoever writes the deployment ticket should know that scaling the
    refresh-worker deployment past one replica will not fail loudly; it will just
    multiply upstream load.
12. **`start()`/`_run()` on a thread became a blocking `run()`** plus SIGTERM and
    SIGINT handlers in the `run_refresh_worker` entrypoint.

### Middleware

13. **`ErrorHandlingMiddleware` was never built.** `Service.handle_request` covers
    the same ground, and the `BaseLogrepError` hierarchy the plan specified it
    should serialise has been deleted.
14. **`PublicAccessMiddleware` was never built.** Deferred with the public-facing
    release.

### File layout

15. **Everything sits at the package root.** Every path in the plan is
    `web_app/`-prefixed.
16. **`adapters/consdb.py` became two files** (`consdb_exposures.py`,
    `consdb_visits.py`) and **`adapters/jira.py` became two**
    (`jira_obs.py`, `jira_block.py`).
17. **`rubin_nights_efd.py` and `rubin_nights_visits.py` were never created.**
    Visits come from ConsDB directly; the EFD is reached through
    `RubinNightsClientsMixin` from the dome, obs-status and context adapters.
18. **`VisitOverheadAdapter` appears nowhere in the plan.** It emerged from
    splitting `get_time_accounting` along its cacheable seam.
19. **`cache_ttl.py` and `redis_client.py` are new modules.** The plan had TTLs
    inline in `_ttl` and the Redis client "instantiated once at application
    startup"; both were centralised. The plan also had two TTL tiers where there
    are now three, each with a separate client and Redis value.
20. **The `utils.py` split and the `web_app` flatten are not in the plan at all.**
    Neither was anticipated; both fell out of the legacy layer shrinking to nothing.
21. **Service modules dropped their `_service` suffix** — `services/almanac.py`,
    not `web_app/services/almanac_service.py`.

### Scope

22. **The authentication open question resolved as Option A** (service-level
    credentials). The plan marked it "unresolved and needs input before
    implementation begins", and asked for confirmation from the RSP/services team
    on whether upstream APIs support service accounts.

    The resolution went one step further than the plan's Option A described. Option
    A was written as a change to *request-time threading* — "each adapter is
    configured at startup with a service account token … no token is passed at
    request time … requires no changes to adapter method signatures". It said
    nothing about the resolution chain underneath, because the plan's author did
    not know that chain already ignored the user's token in every deployment.
    Establishing that ([§2](#2-the-authentication-model-changed)) made the RSP
    notebook branches and the header fallback dead weight, so
    `retrieve_access_token` collapsed to a single environment lookup and a missing
    token became a 500 rather than a 401.
23. **Sync/async landed differently than specified.** The plan kept
    `run_in_threadpool` at the call sites. The handlers themselves are now sync
    `def` and FastAPI's threadpool does the work; `run_in_threadpool` no longer
    appears in the codebase.
24. **`/mock-exposures` was listed as an endpoint to keep** — one of three
    deliberately outside the pattern. It was deleted instead.
25. **Three adapters the plan described as "moved" were rewrites.**
    `ZephyrAdapter` (which the plan had wrapping `ZephyrInterface`),
    `ConsdbCachedAdapter`, and `AlmanacCachedAdapter` all needed reimplementation
    rather than relocation.
26. **Metrics and instrumentation were not implemented.** The plan's §3 anticipated
    cache hit rates, fetch durations and per-adapter error rates instrumented from
    the base class, extending to Prometheus and Grafana. Only the refresh worker's
    per-cycle logging exists. The single-point-of-instrumentation argument still
    holds, and the hook is still `CachedAdapter._fetch_cached`.
