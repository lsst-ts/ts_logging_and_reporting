# Backend caching refactor — before/after performance

Before: `c954701` (2026-07-29T20:23:40+00:00) — pre-refactor `develop`
After: `6609308` (2026-08-02T03:50:11+00:00) — `experiemental/cache-refactor`

Both runs: `dayObsStart=20260611`, 7-day window, `instrument=LSSTCam`, against
uvicorn directly on `127.0.0.1:8080`.

This document reports what the caching refactor did to endpoint latency, and what
the numbers do and do not support. The table is generated output; everything after
it is interpretation.

`scripts/perf_test.py` times requests against each endpoint before and after the
refactor and joins the two captures into the table below. Scenario definitions,
sampling and the constraints on a valid run are documented in that script.

**Reading the columns.**

- **p50** is per-request latency over **successful requests only** — failures are
  counted separately in the JSON captures and excluded from the percentile. Where a
  scenario has many failures this makes p50 unreliable, and those cases are called
  out below.
- **wall p50** is the median time for a whole burst round of 10 to complete, so it
  is what a user facing a cold dashboard actually waits. It appears only on burst
  rows. Wall time near the single-request p50 means genuine parallelism; wall time
  near `burst_size × p50` means the requests were serialised.
- **change / wall change** are relative to the baseline row, negative being faster.
  Baseline requests are always cold, so each after-scenario is compared against the
  cold baseline of the same shape — matching range length, and burst against burst.
  Every 7-day non-burst row therefore shares one `cold-7day` baseline.

**NB — `/block-details` was captured in a separate run.** A harness defect meant its
values were not recorded correctly during the main sweep, so its six rows come from
a re-run against the same two commits, the same date window and the same key set,
but not the same wall-clock session as the other endpoints. Its numbers are
internally consistent (before against after) and safe to read as such; only
cross-endpoint comparisons involving `/block-details` inherit the extra variable of
a different upstream moment.

---

## Results

| Endpoint | Scenario | before p50 (s) | after p50 (s) | change | before wall p50 (s) | after wall p50 (s) | wall change |
|---|---|---|---|---|---|---|---|
| almanac | cold-1day | 0.902 | 0.908 | +1% |  |  |  |
| almanac | cold-7day | 6.008 | 6.02 | +0% |  |  |  |
| almanac | hot-7day | 6.008 | 0.01 | -100% |  |  |  |
| almanac | partial-rolling-7day | 6.008 | 0.896 | -85% |  |  |  |
| almanac | partial-extension-7day | 6.008 | 5.14 | -14% |  |  |  |
| almanac | cold-7day-burst | 51.409 | 6.382 | -88% | 59.939 | 6.453 | -89% |
| almanac | hot-7day-burst | 51.409 | 0.063 | -100% | 59.939 | 0.102 | -100% |
| almanac | partial-rolling-7day-burst | 51.409 | 1.027 | -98% | 59.939 | 1.054 | -98% |
| narrative-log | cold-1day | 1.073 | 1.069 | -0% |  |  |  |
| narrative-log | cold-7day | 1.667 | 1.762 | +6% |  |  |  |
| narrative-log | hot-7day | 1.667 | 0.04 | -98% |  |  |  |
| narrative-log | partial-rolling-7day | 1.667 | 0.931 | -44% |  |  |  |
| narrative-log | partial-extension-7day | 1.667 | 1.692 | +1% |  |  |  |
| narrative-log | cold-7day-burst | 16.351 | 2.174 | -87% | 19.689 | 2.283 | -88% |
| narrative-log | hot-7day-burst | 16.351 | 0.31 | -98% | 19.689 | 0.347 | -98% |
| narrative-log | partial-rolling-7day-burst | 16.351 | 1.19 | -93% | 19.689 | 1.22 | -94% |
| exposure-entries | cold-1day | 0.723 | 0.716 | -1% |  |  |  |
| exposure-entries | cold-7day | 0.723 | 0.727 | +1% |  |  |  |
| exposure-entries | hot-7day | 0.723 | 0.009 | -99% |  |  |  |
| exposure-entries | partial-rolling-7day | 0.723 | 0.716 | -1% |  |  |  |
| exposure-entries | partial-extension-7day | 0.723 | 0.724 | +0% |  |  |  |
| exposure-entries | cold-7day-burst | 7.122 | 0.814 | -89% | 7.157 | 0.891 | -88% |
| exposure-entries | hot-7day-burst | 7.122 | 0.06 | -99% | 7.157 | 0.097 | -99% |
| exposure-entries | partial-rolling-7day-burst | 7.122 | 0.774 | -89% | 7.157 | 0.806 | -89% |
| exposures | cold-1day | 14.655 | 9.211 | -37% |  |  |  |
| exposures | cold-7day | 28.511 | 31.706 | +11% |  |  |  |
| exposures | hot-7day | 28.511 | 0.539 | -98% |  |  |  |
| exposures | partial-rolling-7day | 28.511 | 5.738 | -80% |  |  |  |
| exposures | partial-extension-7day | 28.511 | 25.81 | -9% |  |  |  |
| exposures | cold-7day-burst | 280.764 | 36.925 | -87% | 280.839 | 37.095 | -87% |
| exposures | hot-7day-burst | 280.764 | 5.795 | -98% | 280.839 | 6.075 | -98% |
| exposures | partial-rolling-7day-burst | 280.764 | 8.362 | -97% | 280.839 | 8.551 | -97% |
| obs-status | cold-1day | 4.573 | 0.208 | -95% |  |  |  |
| obs-status | cold-7day | 4.597 | 0.231 | -95% |  |  |  |
| obs-status | hot-7day | 4.597 | 0.02 | -100% |  |  |  |
| obs-status | partial-rolling-7day | 4.597 | 0.203 | -96% |  |  |  |
| obs-status | partial-extension-7day | 4.597 | 0.216 | -95% |  |  |  |
| obs-status | cold-7day-burst | 45.32 | 0.922 | -98% | 45.421 | 0.98 | -98% |
| obs-status | hot-7day-burst | 45.32 | 0.143 | -100% | 45.421 | 0.2 | -100% |
| obs-status | partial-rolling-7day-burst | 45.32 | 0.363 | -99% | 45.421 | 0.397 | -99% |
| multi-night-visit-maps | cold-1day | 22.009 | 17.461 | -21% |  |  |  |
| multi-night-visit-maps | cold-7day | 36.888 | 34.037 | -8% |  |  |  |
| multi-night-visit-maps | hot-7day | 36.888 | 18.022 | -51% |  |  |  |
| multi-night-visit-maps | partial-rolling-7day | 36.888 | 19.539 | -47% |  |  |  |
| multi-night-visit-maps | partial-extension-7day | 36.888 | 30.514 | -17% |  |  |  |
| multi-night-visit-maps | cold-7day-burst | 201.575 | 224.603 | +11% | 300.139 | 300.143 | +0% |
| multi-night-visit-maps | hot-7day-burst | 201.575 | 265.896 | +32% | 300.139 | 300.148 | +0% |
| multi-night-visit-maps | partial-rolling-7day-burst | 201.575 | 285.994 | +42% | 300.139 | 300.16 | +0% |
| static-visit-map | cold-1day | 13.327 | 8.273 | -38% |  |  |  |
| static-visit-map | cold-7day | 23.27 | 19.306 | -17% |  |  |  |
| static-visit-map | hot-7day | 23.27 | 4.506 | -81% |  |  |  |
| static-visit-map | partial-rolling-7day | 23.27 | 6.888 | -70% |  |  |  |
| static-visit-map | partial-extension-7day | 23.27 | 17.907 | -23% |  |  |  |
| static-visit-map | cold-7day-burst | 233.37 | 57.043 | -76% | 234.623 | 54.418 | -77% |
| static-visit-map | hot-7day-burst | 233.37 | 41.239 | -82% | 234.623 | 42.808 | -82% |
| static-visit-map | partial-rolling-7day-burst | 233.37 | 41.354 | -82% | 234.623 | 42.127 | -82% |
| block-details | cold-all-keys | 2.122 | 1.427 | -33% |  |  |  |
| block-details | hot-all-keys | 2.122 | 0.01 | -100% |  |  |  |
| block-details | partial-keys | 2.122 | 0.864 | -59% |  |  |  |
| block-details | cold-all-keys-burst | 4.684 | 2.139 | -54% | 5.658 | 2.188 | -61% |
| block-details | hot-all-keys-burst | 4.684 | 0.069 | -99% | 5.658 | 0.112 | -98% |
| block-details | partial-keys-burst | 4.684 | 1.28 | -73% | 5.658 | 1.339 | -76% |

---

## Analysis

### Noise floor

Five rows measure code paths the refactor did not change (`almanac cold-1day`,
`almanac cold-7day`, `exposure-entries cold-1day`, `exposure-entries cold-7day`,
`narrative-log cold-1day`). All five land within ±1%, which sets the reproducibility
floor for the stable endpoints. Endpoints with wider intrinsic spread —
`narrative-log` at 7 days, where p95 is 1.6× p50 — need more headroom than that
before a few percent means anything.

### The headline result: concurrency stopped multiplying

The clearest cross-cutting number in the capture is not in the table. Divide each
burst p50 by the sequential cold p50 for the same endpoint and range — the factor
by which ten simultaneous requests cost more than one:

| Endpoint | before | after |
|---|---|---|
| almanac | 8.6× | 1.06× |
| narrative-log | 9.8× | 1.23× |
| exposure-entries | 9.9× | 1.12× |
| exposures | 9.8× | 1.16× |
| obs-status | 9.9× | 4.0× |
| block-details | 2.2× | 1.50× |

Before the refactor, every dayobs endpoint sat at 8.6–9.9× for a burst of 10: ten
concurrent requests for *the same data* did ten independent lots of upstream work,
so the burst cost ten times the single request. After, five of six sit between 1.06×
and 1.5×, meaning the marginal cost of the 2nd through 10th simultaneous request for
the same key is close to zero. That is the single-flight lock behaving as designed,
and it is the property that matters operationally — a shift change where several
people load the same night at once used to be the worst case and is now nearly free.

`obs-status` at 4.0× is an artefact of its denominator rather than a weaker result.
The ratio divides by a sequential cold path that is now 0.231 s, so the costs a
burst still pays regardless — ten connections, ten serialisations, ten response
writes — stop being negligible against it. Subtracting rather than dividing puts it
back in family:

| Endpoint | burst p50 − sequential cold p50 |
|---|---|
| exposure-entries | 0.087 s |
| almanac | 0.362 s |
| narrative-log | 0.412 s |
| obs-status | 0.691 s |
| block-details | 0.712 s |
| exposures | 5.219 s |

`obs-status` at 0.691 s sits alongside `block-details` at 0.712 s, and stands out in
the ratio column only because its baseline is 26× smaller than `almanac`'s.
(`exposures` is the one real outlier, for an understood reason: ten concurrent
2.1 MB responses.) In absolute terms its burst went from 45.3 s to 0.92 s, and the
whole round of ten completes in 0.98 s wall — about 0.1 s per request of handling
that does not parallelise.

Underneath this, `obs-status` is dominated by per-request cost rather than by data
volume, and always was: its marginal cost is 0.0040 s/night before and 0.0038 after,
so a payload that doubles from 1 day to 7 barely moves it. What the refactor changed
is the *magnitude* of that per-request term, not its dominance — from ~4.57 s of
upstream work that nothing shared, and that a burst therefore multiplied by ten, to
~0.1 s of irreducible request handling.

The general caution: once the denominator approaches the fixed per-request cost, a
ratio stops measuring what it measured on the slower endpoints. Read absolute
numbers there.

The wall-clock columns corroborate this independently of p50. On every burst row
except the two map endpoints, wall p50 sits within a few percent of the per-request
p50 — 6.453 vs 6.382 for `almanac`, 37.095 vs 36.925 for `exposures`, 0.98 vs 0.922
for `obs-status`. A whole round of ten finishing in about the time of one request is
what genuine parallelism looks like. The baseline shows the opposite: `almanac`
59.939 s wall against a 6.008 s sequential request.

### A second saving, unrelated to caching: rubin_nights clients are built once

Four endpoints shed a near-constant 4.4–5.4 s that has nothing to do with Redis.
The old code called `get_clients(auth_token=...)` *inside the request path* — six
such call sites in `web_app/services/rubin_nights_service.py`, including
`get_obs_status_events` and `get_visits` — so every request rebuilt the rubin_nights
connection clients and repeated credential discovery. The refactor moved this to
`RubinNightsClientsMixin`, where `_clients` and `_efd_client` are
`functools.cached_property` on a process-cached adapter singleton: once per process
rather than once per request.

Cold 1-day is the cleanest probe of a fixed cost, and the saving appears on exactly
the endpoints that made that call:

| Endpoint | old path via `get_clients` | cold-1day change |
|---|---|---|
| obs-status | yes | −4.37 s |
| multi-night-visit-maps | yes | −4.55 s |
| static-visit-map | yes | −5.05 s |
| exposures | yes | −5.44 s |
| almanac | no | +0.006 s |
| narrative-log | no | −0.004 s |
| exposure-entries | no | −0.007 s |

The three endpoints that never touched `get_clients` moved by under 10 ms. That the
effect is a near-constant offset, present only where that call was and independent
of how much data each endpoint returns, is the signature of per-request setup cost
rather than of anything data-dependent.

This matters for interpretation: a substantial part of the improvement on
`obs-status` and the two map endpoints would have been available from this change
alone, without any cache. Attributing it to caching overstates what the cache does.

### `almanac` — per-day chunking is exactly linear

`almanac` is the cleanest demonstration that the cache is chunked per dayobs and
that cost tracks *missing* days rather than requested days. Its cold 7-day cost is
6.020 s, i.e. 0.860 s per night. Predicting each partial scenario from that constant:

| Scenario | Days to fetch | Predicted | Observed |
|---|---|---|---|
| `hot-7day` | 0 | ~0 | 0.010 s |
| `partial-rolling-7day` | 1 | 0.86 s | 0.896 s |
| `partial-extension-7day` | 6 | 5.16 s | 5.140 s |
| `cold-7day` | 7 | 6.02 s | 6.020 s |

The six-day prediction lands within 0.4% of observation and the one-day within 4%.
This is the property the whole design rests on, and `almanac` — being pure
computation with no upstream variability — measures it with almost no confounds.

Note that `partial-extension-7day` at −14% is *correct behaviour looking
unimpressive*: widening a 1-day window to 7 days genuinely requires six nights of
work, and no cache can avoid it. The scenario exists to confirm the cache does not
pretend otherwise.

### `narrative-log`, `exposure-entries`, `block-details` — cache works, no surprises

All three behave as the design predicts, with hot reads at 1–4% of the cold cost
(`narrative-log` 1.667 → 0.040 s; `exposure-entries` 0.723 → 0.009 s;
`block-details` 2.122 → 0.010 s) and partial scenarios landing between hot and cold
in proportion to what is missing.

`narrative-log cold-7day` at +6% is within its own run-to-run spread rather than a
regression: its p95 is 1.6× its p50 on both sides (2.494 s before, 2.804 s after)
and the min/max ranges overlap heavily (1.482–2.799 vs 1.497–3.123). Do not read a
few percent on this endpoint as signal.

`block-details` is the one endpoint whose baseline burst was *not* fully serialised
(2.2×, not ~10×) because per-key upstream requests already interleaved. Its
improvement is correspondingly more modest but still substantial, and the
`partial-keys` scenarios confirm cost scales with the number of *missing* keys
(0.864 s for half the keys, against 1.427 s for all of them) rather than the number
requested.

**Caveat on `exposure-entries`:** every response in both runs was 23 bytes —
`{"exposure_entries":[]}`, an empty result — and its 1-day and 7-day costs are
identical (0.716 vs 0.727 s), confirming the cost is range-independent. This
endpoint returned no data for LSSTCam over 20260611–20260618. Its numbers are
therefore a valid measurement of request plumbing and cache round-trip, and say
nothing at all about behaviour on a populated range. Re-run against a window known
to contain exposure-log entries before drawing conclusions about it.

### `obs-status` — the gain is not from caching

`obs-status` improved by ~95% on *every* scenario including the cold ones, which
flush Redis before each request. A cold path cannot be faster because of a cache.
The cold 1-day cost fell 4.573 → 0.208 s (a factor of 22) and cold 7-day
4.597 → 0.231 s (a factor of 20).

Both before and after, the endpoint is near-flat in range length (4.573 vs 4.597 s
before; 0.208 vs 0.231 s after), so the cost is dominated by a fixed per-request
term in both cases, and it is that fixed term that fell by a factor of ~20. That
fall is the rubin_nights client construction saving described above: the per-night
marginal cost is unchanged (0.0040 → 0.0038 s/day), so the EFD query costs what it
always did and only the per-request constant moved.

### `exposures` — large wins, and one genuine cold regression

The wins are unambiguous: hot 7-day 28.511 → 0.539 s, and the burst went from
280.764 s to 36.925 s with wall time from 280.839 s to 37.095 s. Ten concurrent cold
requests for a full week now cost 1.16× a single one.

The regression is real and should not be waved away: **`cold-7day` went from
28.511 s to 31.706 s, +11%**, well outside the ±1% noise floor. Meanwhile
`cold-1day` improved 37% (14.655 → 9.211 s). Two effects pull in opposite
directions:

| | fixed (s) | marginal (s/day) |
|---|---|---|
| before | 12.35 | 2.31 |
| after | 5.46 | 3.75 |

The fixed cost fell 6.88 s — the client construction saving above, plus roughly
2.5 s of other per-request work in rubin_nights' `get_visits` that these timings do
not separate — while **per-night cost rose 1.44 s/day, +62%**. Crossover is at
~4.8 days: shorter ranges are faster, longer ones slower, and 7 days sits just past
it. Both builds issue one query per contiguous run, so this is not extra round
trips; the likely cause is query width. `ConsdbExposuresAdapter._fetch_run` selects
`e.*, q.*` plus an EFD left-join — the full exposure ⋈ quicklook ⋈ EFD record,
because one entry serves both `/exposures` and `/data-log` — where the old
`get_visits` used rubin_nights' curated column set. Wider rows scale transfer,
`make_json_safe` conversion and the Redis write with the number of nights.

A two-parameter model fitted to two points fits both by construction, so the table
above is interpolation, not confirmation. It does make a falsifiable prediction:
cold 3-day and 5-day runs should show ~break-even at five days
(`--endpoints exposures --runs 15`). That would either confirm the linear picture or
expose a nonlinearity the two existing points cannot distinguish.

Also note the serialisation floor this endpoint exposes: `hot-7day-burst` is 5.795 s
p50 / 6.075 s wall despite every byte coming from cache, because ten concurrent
responses of 2.1 MB each must still be serialised — roughly 21 MB of JSON in about
six seconds. Caching cannot reduce that, and it is the same mechanism that dominates
the map endpoints below.

### `multi-night-visit-maps` and `static-visit-map` — rendering is not cached

Both map endpoints improve across every sequential scenario, but by far less than
their peers, and for the same structural reason: only the ConsDB visit records are
cached. Augmentation, figure construction and serialisation run on every request,
hot or cold. `multi-night-visit-maps` still costs **18.022 s fully hot** (against
0.01–0.54 s for every non-map endpoint) and `static-visit-map` **4.506 s**. The
cache removed the fetch; the render is now essentially the whole cost.

**The three `multi-night-visit-maps` burst rows carry no signal and must not be read
as a regression.** The +11% / +32% / +42% figures are an artefact of p50 being taken
over successful requests only. The baseline burst completed **1 request out of 100**
— the other 99 hit the 300 s timeout — so its 201.575 s "median" is a single sample,
effectively the fastest survivor. After the refactor the same bursts completed 2, 6
and 37 respectively, and one of those (162.4 s) was faster than anything the baseline
finished at all. Adding slower-but-successful completions to a one-sample base raises
the median while the endpoint strictly improves; the rolling burst went from 1
completion to 37 under identical load and is reported as +42%.

The wall-clock columns state this plainly: 300.139 s before against 300.143 /
300.148 / 300.160 s after — both sides pinned to the client timeout to within 20 ms.
Both builds saturate, and the correct reading of those three rows is *no measurement*
rather than a change in either direction.

The cause is that the remaining work is CPU-bound Python in a sync handler, so it
serialises on the GIL regardless of cache state; ten concurrent renders exhaust the
timeout however warm the cache is. Deferred, with options costed, in the *Visit-map
rendering is not cached* entry of `REFACTOR_ODDITIES.md` §13.

`static-visit-map`'s bursts also degraded on both sides, but differently and more
worryingly. Its after-side burst rows returned **HTTP 500s at a rate of 62–67 per
100 requests** (the baseline saw 11 HTTP 502s, a different failure). These are
errors, not timeouts, so its −76% / −82% burst figures are likewise computed over a
third of the requests and understate nothing but also prove little.

The cause has been confirmed separately, and it is a correctness defect rather than a
throughput one. Rendering one fixed visits frame concurrently and comparing the
returned PNG bytes against a single-threaded reference — identical input must give
identical output — gives, over ten threads × ten rounds:

| | wrong renders |
|---|---|
| concurrent | 92–98 / 100 |
| serialised behind a lock | 0 / 100 |

The locked run being byte-identical on every render establishes that the function is
deterministic for fixed input, so the comparison is sound and concurrency is the only
remaining variable. Of the concurrent failures, ~27% raise
`IndexError: list index out of range` and the rest return a *wrong image with a 200*.

The shared state is pyplot's global active-figure and active-axes pointers.
`services/static_visit_map.py` writes them at `plt.figure(plot["SkyMap"])`,
`plt.sca(ax)` (line 117) and `plt.close(fig)`, and FastAPI dispatches this sync
handler into a threadpool. Tracing every mutation across the ten threads recorded 384
writes, of which **155 were one thread taking the active figure or axes from another
mid-render**. The pyplot use is not confined to those three lines: the trace saw 100
no-argument `plt.figure()` calls and 142 `plt.figure(<Figure>)` calls against only
100 renders, so MAF and healpy create and switch figures themselves. `plt.sca` is
likewise load-bearing rather than incidental — `hp.graticule()` takes no axes
argument and draws on `plt.gca()`. Rewriting the service against matplotlib's
object-oriented API therefore cannot fix this; the fix is a lock around the render,
or a process pool.

This also explains the burst payload sizes recorded in `AFTER.json` — 74,842 and
146,754 bytes against 164,450 for the identical sequential request — which were
previously only an indication. The HTTP 500s are very likely the same `IndexError` 
surfacing through the error handler.

---

## Summary

- Six of eight endpoints show large, well-behaved improvements, with concurrent
  cost collapsing from ~10× a single request to ~1.1–1.5×.
- Per-day chunking is verified quantitatively on `almanac` to within 4%, and
  qualitatively everywhere else via the partial scenarios.
- A 4.4–5.4 s per-request saving on four endpoints comes from building the
  rubin_nights clients once per process rather than once per request, not from the
  cache. It accounts for essentially all of `obs-status`'s ~20× gain.
- `exposures cold-7day` is a genuine 11% regression with a plausible and testable
  cause (a wider cached record raising per-night cost past a ~5-day crossover).
- The two map endpoints are the outstanding work: rendering is outside the cache,
  and `static-visit-map` additionally returns wrong images under concurrency —
  confirmed, and caused by pyplot global state rather than load.
- `exposure-entries` returned no data in this window and needs re-measuring on a
  populated range.
