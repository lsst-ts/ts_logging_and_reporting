# Service and adapter infrastructure

This document describes how the backend serves endpoint data: the split
between the service and adapter layers, how caching works, and what you
have to write to add a new endpoint.

---

## 1. Overview: services, adapters, and the request path

The backend has two layers between a FastAPI route and the outside
world.

**Adapters** ([§2](#2-anatomy-of-an-adapter-cache-base--client--mixins))
own data acquisition and caching. Each adapter wraps
exactly one upstream source — the Night Report API, ConsDB, Jira,
Zephyr Scale, the EFD, or a local computation such as the almanac — and
exposes it as a cache-backed lookup keyed by dayobs (or by opaque ID).
Every adapter fetch goes through Redis first; the upstream is contacted
only for keys that are not already cached. Adapters do not know what an
endpoint looks like.

**Services** ([§3](#3-anatomy-of-a-service)) own collation and response
shaping. A service holds one or more adapters, asks each for the range
it needs, merges the results, and returns the JSON-serialisable payload
the frontend consumes. Services never touch Redis and never issue an
HTTP request to an upstream themselves.

The consequences worth internalising:

- Caching is a property of the *source*, not of the *endpoint*. Two
  endpoints backed by the same upstream share one cache entry — for
  example `/exposures` and `/data-log` both read the single
  `consdb_exposures` entry for a night, projecting different columns
  from it.
- Cache entries are per-dayobs buckets, so a request for a five-night
  range is five independent lookups and only the uncached nights cost
  an upstream call. A range that overlaps a previously requested range
  reuses the overlap.
- Anything expensive belongs on the adapter side of the line, so that
  it lands in the cache. Anything that depends on the request's exact
  parameters belongs on the service side.

The code layout mirrors this split:

```
python/lsst/ts/logging_and_reporting/
├── main.py                  FastAPI app: routes, middleware registration
├── cache_ttl.py             all TTL constants (Redis and client)
├── redis_client.py          the shared Redis client
├── refresh_worker.py        background cache-warming loop
├── run_refresh_worker.py    worker process entrypoint
├── adapters/
│   ├── __init__.py          factory exports + the refresh-worker registry
│   ├── base_adapters.py     cache bases: the cache loop lives here
│   ├── base_clients.py      transport clients: RestClient, SqlClient
│   ├── mixins.py            cross-adapter behaviour (TTL policy, auth, …)
│   └── <source>.py          one concrete adapter per upstream source
├── services/
│   ├── base_service.py      Service ABC: error mapping, concurrent fetch
│   └── <endpoint>.py        one service per endpoint
├── middleware/              request logging, dayobs validation, Cache-Control
└── utils/                   dayobs maths, collation, serialisation, auth,
                             logging configuration
```

Three middlewares sit in front of the routes:
`RequestLoggingMiddleware` logs each request, times it, and tags
everything it logs with a trace ID (`doc/logging.md`),
`DayobsValidationMiddleware`
rejects malformed or inverted dayobs ranges before they reach a service,
and `CacheControlMiddleware` sets the response's `Cache-Control` header
on the way out (see [§6](#6-ttls-three-data-kinds-two-tiers)).

---

## 2. Anatomy of an adapter: cache base + client + mixins

A concrete adapter is assembled from three kinds of class, none of which
it usually has to modify:

**A cache base class** (`adapters/base_adapters.py`) supplies the cache
loop and defines the key shape:

| Base | Cache key | Public accessor | Subclass implements |
|---|---|---|---|
| `DayobsCachedAdapter` | `<dayobs>` | `fetch(start, end)` | `_fetch_run(run_start, run_end)` |
| `InstrumentDayobsCachedAdapter` | `<instrument>:<dayobs>` | `fetch(instrument, start, end)` | `_fetch_run(instrument, run_start, run_end)` |
| `IdCachedAdapter` | `<id>` | `fetch_by_ids(ids)` | `_fetch_from_source(ids)` |

All three derive from `CachedAdapter`, which holds the machinery they
share: the cache loop and its single-flight locks
([§4](#4-the-single-flight-cache-loop)), key naming, TTL dispatch, and
the `_partition_by_field` helper ([§9](#9-shared-helpers)). The three
differ only in what a key *is*, their public interface, and how a
public call expands into a list of keys.

Use `InstrumentDayobsCachedAdapter` when the upstream query is
per-instrument and cannot be shared. When the upstream returns all
instruments in one response (or is not based on instruments) it 
is cheaper to use `DayobsCachedAdapter` and let the services 
filter — `ExposurelogCachedAdapter` does exactly this, so 
one fetch and one refresh cover every instrument. For adapters
which are not based on a dayobs range, use `IdCachedAdapter`
which allows you to use an arbitrary, opaque ID for each 
element you need to cache.

**A transport client** (`adapters/base_clients.py`) supplies the request
machinery. `RestClient` provides server-URL resolution, per-request
auth token retrieval, timeouts, `_get_json`, `_post_json`, and
`_get_json_paged`. `SqlClient` extends it with ConsDB's `/consdb/query`
endpoint and row shaping. An adapter with no HTTP upstream (the almanac,
computed locally with astroplan; expected exposures, read via
`rubin_sim.sim_archive`) simply has no client.

**Zero or more mixins** (`adapters/mixins.py`) supply cross-cutting
behaviour: `MutableDataMixin` (TTL policy, [§6](#6-ttls-three-data-kinds-two-tiers)),
`JiraApiMixin` (Jira server resolution and Basic auth),
`ConsdbSqlMixin` (request validation and duplicate-column merging for
ConsDB SQL), `RubinNightsClientsMixin` (lazily built, process-cached
`rubin_nights` connection clients).

The declaration order is a real constraint, not a style choice —
Python's MRO resolves left to right, so **mixins first, then the client,
then the cache base**:

```python
class NightReportCachedAdapter(RestClient, DayobsCachedAdapter): ...
class ExposurelogCachedAdapter(MutableDataMixin, RestClient, DayobsCachedAdapter): ...
class JiraObsCachedAdapter(JiraApiMixin, MutableDataMixin, RestClient, DayobsCachedAdapter): ...
class ConsdbExposuresAdapter(ConsdbSqlMixin, SqlClient, InstrumentDayobsCachedAdapter): ...
class ZephyrAdapter(MutableDataMixin, RestClient, IdCachedAdapter): ...
```

---

## 3. Anatomy of a service

One service per endpoint, in `services/<endpoint>.py`, subclassing
`Service` (`services/base_service.py`). Where an adapter is assembled
from several collaborating classes, a service is a single class with a
small fixed surface: the variation between services is in what they
collate, not in how they are built.

**`handle(...)`** *(abstract)* — the work of one request. Each subclass
declares its own typed signature; the base class deliberately
prescribes no parameters, since every endpoint's query parameters
differ. The body is generally the same three moves: fetch from the
adapter(s), merge, and return `self.collate_response(...)`.

**`handle_request(...)`** *(concrete)* — what the endpoint actually
calls. It delegates to `handle` and converts anything raised into an
HTTP response ([§13](#13-failure-semantics)). Endpoints must never call
`handle` directly: that bypasses the error handling, so an 
upstream failure surfaces as an unlogged 500 instead of a 502.

**`collate_response(data)`** *(abstract)* — turns the merged data into
the response payload. Most services return records in a
response-shaped dict, usually via `flatten_sorted`
([§9](#9-shared-helpers)); the payload does not have to be records,
though — `StaticVisitMapService` returns a base64-encoded PNG and
`VisitMapsService` a Bokeh JSON item, both built from the same
per-dayobs adapter data. A service may also give the method
keyword-only arguments beyond `data` — `VisitMapsService` passes
`instrument` and `applet_mode` — since only its own `handle` calls it.

**`fetch_concurrently(tasks)`** *(concrete helper)* — runs several
independent fetches at once, returning each one's result or its
exception ([§9](#9-shared-helpers)).

### Services that do CPU-bound work

Waiting on an upstream is what most services do, and threads are right
for it — that is what `fetch_concurrently` uses. Work that *computes*
rather than waits is the opposite case: under the GIL a thread buys no
throughput at all, and the API process spends the whole render unable to
serve anything else.

Such a service mixes in `WorkerPoolMixin`
(`services/worker_pool_mixin.py`) ahead of `Service`, and calls
`run_in_worker(func, *args)` instead of calling `func` directly. Each
service gets its own pool of worker processes, so a burst on one
endpoint cannot take capacity from another. Both visit map services use it —
`StaticVisitMapService` for the healpix PNG, `VisitMapsService` for the
Bokeh document.

```python
class StaticVisitMapService(WorkerPoolMixin, Service):
    pool_preload = ("lsst.ts.logging_and_reporting.services.static_visit_map",)

    def collate_response(self, data):
        ...
        png_bytes = self.run_in_worker(build_static_visit_map, visits)
```

Four class attributes tune a pool: `pool_workers`, `pool_queue`,
`pool_timeout` and `pool_preload`. Only `pool_preload` is service
specific in practice — naming the service's own module so its
dependencies are imported once at startup and shared, rather than 
per worker or per request.

All the pools share one forkserver, which reads its preload list only
when it starts, so `preload_worker_modules` registers every service's
list together before any pool is built. Setting it per pool would apply
only to whichever pool was built first due to `forkserver` constraints.

Two requirements follow from the work crossing a process boundary. The
callable must be importable by name, so it has to be a module-level
function rather than a method, a closure or a mock; and its arguments
and return value must pickle.

Two things the mixin carries across that boundary for you: each worker
calls `configure_logging()` as it starts, since it never runs an
entrypoint and would otherwise log nothing, and the calling request's
trace ID is pickled over with the call. Both are covered in
`doc/logging.md`.

Pools are started and stopped by the application's lifespan hook, which
reads the `WORKER_POOL_SERVICES` registry in `services/__init__.py`. A
service that gains a pool must be added there
([§11](#11-adding-a-new-endpoint-what-you-must-override)); starting at
application startup rather than on first use keeps the forkserver's
import cost off the first request.

### A complete service, minus imports:

```python
class NightReportService(Service):
    def __init__(self, nightreport_adapter: NightReportCachedAdapter | None = None) -> None:
        self.nightreport_adapter = (
            nightreport_adapter if nightreport_adapter is not None else get_nightreport_adapter()
        )

    def handle(self, day_obs_start: int, day_obs_end: int) -> dict:
        per_day = self.nightreport_adapter.fetch(
            day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
        return self.collate_response(per_day)

    def collate_response(self, data: dict[int, Any]) -> dict:
        return {"reports": flatten_sorted(data, "day_obs")}


@functools.cache
def get_night_report_service() -> NightReportService:
    return NightReportService()
```

That constructor — one named, typed, `None`-defaulting parameter per
adapter — and the `functools.cache` factory are the standard
construction idiom, shared with the adapter layer and described in
[§7](#7-dependency-injection).

### Three rules keep the layer thin:

- **Anything that talks to an upstream belongs in an adapter.**
  Reaching for `requests` inside a service is the signal that the fetch
  should move down a layer, where its result gets cached and shared
  with every other endpoint that needs it.
- **A service holds no per-request state.** Instances are shared across
  concurrent requests ([§7](#7-dependency-injection)); everything a
  request needs travels through arguments and return values.
- **A service decides which of its sources are essential.** With
  multiple adapters, some failures should fail the request and others
  should degrade into an error field
  ([§13](#13-failure-semantics)).

### Why both Service.handle and Service.collate_response?

Having one function for "IO fetches from the upstream world"
and one function for "turn data from the cache into a response"
we are able to test these two parts of the service more easily.
It also keeps the functions small and focused only on what
actually needs to be done at each step.

---

## 4. The single-flight cache loop

`CachedAdapter._fetch_cached(keys)` is the heart of the layer. Every
public accessor — `fetch`, `fetch_by_ids` — reduces its arguments to a
list of keys and hands them here. It returns a dict mapping every
requested key to its value.

The problem it solves: without coordination, N concurrent requests for
an uncached night all miss, all call the upstream, and all write the
same entry. On a cold cache after a deploy, or at the moment a nights
entry is first requested, that is a stampede against a slow external
service. The lock makes exactly one request do the work.

The loop, in order:

1. **Probe.** Every key is looked up with `_check_cache`, which returns
   `(hit, value)`. The boolean matters: a cached JSON `null` is a
   legitimate value (e.g., the Zephyr adapter caches "no such test case" as
   `None`) and must not be mistaken for a miss. Hits are collected; if
   nothing is missing the function returns without touching the
   upstream.

2. **Claim.** For each missing key, `_acquire_lock` does a Redis
   `SET lock:<cache key> "1" NX EX 30`. `NX` makes this atomic: exactly
   one process wins each key. Keys divide into *won* and *lost*.

3. **Re-probe the won keys.** Another request may have stored the entry
   and released its lock in the window between our probe and our lock
   acquisition. Won keys that turn out to be cached are served from the
   cache and their locks released immediately.

4. **Fetch.** The remaining won keys go to `_fetch_from_source` as a
   single batch — one call, not one per key, so an adapter can collapse
   them into one upstream query. Each returned value is stored with
   `_store` and added to the results. The locks are released in a
   `finally`, so a failed fetch does not leave a key locked for the
   full 30 s.

5. **Wait on the lost keys.** `_wait_for_entry` polls the cache every
   `POLL_INTERVAL` (0.1 s) while the holder's lock still exists. When
   the entry appears it is returned. If the lock disappears with no
   entry — the holder's fetch failed, or the holder died — the key goes
   back into the pending set and the whole loop repeats for it, so the
   fetch (and any upstream error) happens in *this* request rather than
   silently yielding no data.

Contracts that fall out of this, and that new adapters must respect:

- **`_fetch_from_source` must return an entry for every key it was
  given.** A key with no data must map to an empty container, not be
  omitted. The loop raises `KeyError` naming the adapter if a key is
  missing, because a silently absent key would be indistinguishable
  from a cache miss and would be re-fetched forever.
- **Any upstream error propagates and fails the entire request.**
  Partial data is never returned and never cached, so a cached entry is
  always a complete answer for its key. What the service layer does
  with that error is [§13](#13-failure-semantics).
- **Values must be JSON-serialisable** — see
  [§14](#14-serialisation-constraints-on-cached-values).

`LOCK_TTL` (30 s) exists only so a crashed lock holder cannot block a
key forever; it must exceed the slowest expected upstream fetch. If a
lock does expire mid-fetch and another request re-acquires it, the
original holder's `_release_lock` deletes the new holder's lock — the
cost is at worst one redundant upstream call.

---

## 5. Cache keys and the Redis client

Cache keys are derived from the adapter name and dayobs number or
opaque ID

```
adapter:<adapter name>:<key>                the entry
lock:adapter:<adapter name>:<key>           its single-flight lock
# e.g., 
adapter:nightreport:20250101                DayobsCachedAdapter
adapter:consdb_exposures:lsstcam:20250101   InstrumentDayobsCachedAdapter
adapter:zephyr:BLOCK-T123                   IdCachedAdapter
```

The `name` class attribute is therefore load-bearing: it namespaces
every entry the adapter owns, it is what the `RefreshWorker`
([§12](#12-the-refresh-worker)) logs on failure. Names must be unique
across adapters.

This scheme makes the cache directly inspectable, which is
the fastest way to answer "is this stale data or a bad fetch?":

```
redis-cli --scan --pattern 'adapter:nightreport:*'
redis-cli ttl adapter:nightreport:20250101
redis-cli del adapter:nightreport:20250101     # force a re-fetch
```

All adapters and share **one** client instance,
created by `get_redis_client()` (a `functools.cache` singleton, the
pattern used for every factory in the codebase —
[§7](#7-dependency-injection)) so the process holds a single connection
pool. The server is configured as a
cache, not a datastore: bounded `maxmemory` with `allkeys-lru` eviction
and persistence disabled. Eviction is safe precisely because every entry
is reconstructible from its upstream.

Nothing in the cache layer depends on the client being a real 
`redis.Redis` — and two stand-ins in the repo take advantage of that:

- **`DisabledRedis`** (`redis_client.py`), returned by
  `get_redis_client()` when `ND_CACHING_DISABLE_REDIS` is set. Every
  read misses and every write is dropped, so the app runs as though the
  cache were permanently empty. Use it to observe raw upstream
  behaviour — timings, request volume, error handling — without
  stopping the Redis server or changing any code.
- **`FakeRedis`** (`tests/conftest.py`), exposed as the `fake_redis`
  fixture. An in-memory cache faithful enough to test caching
  behaviour against: entries really expire, the TTL used for each key
  is recorded so TTL policy can be asserted, and it is thread-safe so
  the concurrent single-flight tests exercise it directly. The test
  suite therefore needs no Redis server.

---

## 6. TTLs: three data kinds, two tiers

Every cached datum is one of three kinds:

- **Historic** — fully in the past and immutable: ephemeris, a closed
  night's exposure records.
- **Today** — changes as the night progresses.
- **Mutable** — can change on any dayobs: log messages, Jira tickets,
  Zephyr test cases. Someone can edit a narrative log entry for a night
  three weeks ago.

Each kind has two lifetimes, and they stack. The **Redis TTL** governs
the adapter's entry; the **client TTL** is served as
`Cache-Control: max-age` by `CacheControlMiddleware` and governs
browser and nginx caches. Because a response built from a nearly
expired Redis entry can then sit in a client cache for its full
max-age, the client TTLs are deliberately much shorter — only the Redis
copy can be flushed by hand.

| Kind | Client (`max-age`) | Redis |
|---|---|---|
| Historic | `HISTORIC_TTL` 1 day | `HISTORIC_TTL_REDIS` 30 days |
| Today | `TODAY_TTL` 5 min | `TODAY_TTL_REDIS` 15 min |
| Mutable (past dayobs) | `MUTABLE_TTL` 5 min | `MUTABLE_TTL_REDIS` 1 hour |

All six constants live in `cache_ttl.py`; change them there and nowhere
else.

`TODAY_TTL` doubles as the `RefreshWorker`'s default interval, so a
client is never served data staler than one refresh cycle.
`TODAY_TTL_REDIS` must comfortably exceed that interval so today's entry
cannot expire between cycles; since the worker overwrites the entry
every interval regardless of remaining TTL, its length does not add
staleness — it only bounds how stale the entry can get if the worker
stalls entirely.

### How an adapter picks its TTL

`_store` calls `self._ttl(key)`, whose default implementation is:

```python
def _ttl(self, key) -> int:
    return TODAY_TTL_REDIS if self._is_today(key) else HISTORIC_TTL_REDIS
```

`_is_today` is the single point at which TTL policy interrogates a key,
and each cache base answers for its own key shape:
`DayobsCachedAdapter` compares against `current_dayobs()`;
`InstrumentDayobsCachedAdapter` splits the composite key first;
`IdCachedAdapter` returns `False` unconditionally, since an ID-keyed
record has no today/historic split.

**`MutableDataMixin`** replaces the historic branch:

```python
def _ttl(self, key) -> int:
    return TODAY_TTL_REDIS if self._is_today(key) else MUTABLE_TTL_REDIS
```

Note what it does *not* do: it leaves `_is_today` to the base class, so
one mixin works correctly under all three cache bases, and today's entry
keeps `TODAY_TTL_REDIS` wherever a today exists. Mix it in for any
adapter whose records can be edited after the fact — for example
exposurelog, jira_obs, etc. The set of adapters carrying
this mixin should stay in step with `_MUTABLE_PATHS` in
`middleware/cache_control.py`, which applies the matching client-side
policy per endpoint.

An adapter with a genuinely different lifetime overrides `_ttl`
outright. `AlmanacCachedAdapter` is the one example: twilight
and moon events for tonight are as immutable as last year's,
unless something has gone _drastically_ wrong, so it returns
`HISTORIC_TTL_REDIS` for every key including today's.

---

## 7. Dependency injection

All layers use the same pattern: a process-wide singleton with an
override seam for tests.

**Endpoint → service** uses FastAPI's `Depends`:

```python
@app.get("/night-reports")
def read_night_reports(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_night_report_service),
):
    return service.handle_request(dayObsStart, dayObsEnd)
```

`get_night_report_service` is a `@functools.cache` factory, so the
service is built once on first request and reused. Going through
`Depends` rather than calling the factory inline means the dependency
can be replaced per test with `app.dependency_overrides`.

**Service → adapter** and **adapter → adapter** use constructor
injection with named, typed parameters that default to `None`, each
falling back to its factory when omitted — the shape shown in
[§3](#3-anatomy-of-a-service). Production code constructs the service
with no arguments and gets the shared singletons; a test passes exactly
the mocks it needs and lets the rest default. `ExposuresService` shows
the multi-adapter form — four parameters, same idiom for each.

Adapter factories follow the same pattern, and are where composition
([§8](#8-adapter-composition)) is wired:

```python
@functools.cache
def get_nightreport_adapter() -> NightReportCachedAdapter:
    return NightReportCachedAdapter(get_redis_client())

@functools.cache
def get_visit_overhead_adapter() -> VisitOverheadAdapter:
    return VisitOverheadAdapter(get_redis_client(), get_consdb_exposures_adapter())
```

Every adapter takes the Redis client as its first constructor argument;
`RestClient` subclasses accept an optional `server_url` after it
(defaulting to the deployment's resolved server —
[§15](#15-upstream-authentication-and-server-resolution)), and
individual adapters add their own tuning parameters — page size, record
limit, observatory site.

### Singletons lifetime and shared state.

These singletons are shared across concurrent requests. Several threads 
are therefore inside the same service and adapter instances at once. This leads 
to a critical requirement:

**A service or adapter must NOT keep per-request mutable state on `self`.**

Everything a request needs flows through arguments and return values.
What instances legitimately hold is per-process configuration (server URL,
page size, a composed adapter) and lazily built shared resources —
`functools.cached_property` for the `rubin_nights` clients, so credential
discovery happens once rather than per fetch.

---

## 8. Adapter composition

Two composition shapes exist, and they solve different problems.

### A service holding several adapters

The common case: an endpoint needs data from unrelated sources.
`ExposuresService` holds four adapters (ConsDB exposures, dome, visit
overhead, almanac) and fans three of them out concurrently:

```python
inclusive_end = add_or_subtract_dayobs_days(day_obs_end, -1)
fetched = self.fetch_concurrently({
    "consdb":   lambda: self.consdb_adapter.fetch(instrument, day_obs_start, inclusive_end),
    "dome":     lambda: self.dome_adapter.fetch(day_obs_start, inclusive_end),
    "overhead": lambda: self.overhead_adapter.fetch(instrument, day_obs_start, inclusive_end),
})
```

Because `fetch_concurrently` returns either a result or the exception
per task ([§9](#9-shared-helpers)), the service is able to decide which sources
are essential. In the example `ExposuresService` a ConsDB failure re-raises and fails the request —
exposures are the payload — while dome and time-accounting failures
degrade into `open_dome_error` / `time_accounting_error` fields, so a
flaky EFD does not take the endpoint down. `BlockDetailsService` uses
the same helper across Zephyr and Jira, reporting a single-source
failure in `errors` and failing only if both sources fail.


### An adapter holding another adapter

Used when a derived, expensive computation is itself worth caching, and
its input is already cached. `VisitOverheadAdapter` caches per-visit
slew and overhead times: the kinematic slew model needs the EFD on top
of the full visit sequence. It gets that visit sequence from the ConsDB 
exposures adapter rather than issuing its own query:

```python
class VisitOverheadAdapter(RubinNightsClientsMixin, InstrumentDayobsCachedAdapter):
    name = "visit_overhead"

    def __init__(self, redis_client, exposures_adapter):
        super().__init__(redis_client)
        self._exposures_adapter = exposures_adapter

    def _fetch_run(self, instrument, run_start, run_end):
        per_day = self._exposures_adapter.fetch(instrument, run_start, run_end)
        ...
```

The inner `fetch` is an ordinary cached call, so on a warm cache the
outer adapter's miss costs no ConsDB query at all. Both adapters cache
independently and can be refreshed independently.

The one thing composition adds is an ordering consideration for the
refresh worker: `REFRESH_ADAPTERS` in `adapters/__init__.py` places
`visit_overhead` immediately after `consdb_exposures`, so each cycle
builds overhead from exposures that were refreshed moments earlier in
the same cycle, rather than exposures that are one cycle old.

Composition is worth reaching for when the derived value is expensive
*and* reusable. If it is cheap, or specific to one endpoint's
parameters, do it in the service instead — `ExposuresService`'s
twilight-windowed reduction over the cached overhead rows is exactly
that case, since the window depends on the requested range.

---

## 9. Shared helpers

Several helpers are available for common tasks in both the Service
and Adapter layer.

**`Service.fetch_concurrently(tasks) -> dict[str, Any]`** runs a dict of
zero-argument thunks (for example, adapter.fetch lambdas) 
on a `ThreadPoolExecutor` sized to the number of tasks, and maps 
each name to its return value *or to the exception it raised*. 
Callers must therefore `isinstance(result, Exception)`-check
before use — it lets one service treat a failure as
fatal and another treat it as degradable.

**`CachedAdapter._partition_by_field(rows, key="day_obs") -> dict`**
buckets a flat list of rows by a field value or by a callable applied to
each row. It is the last step of nearly every `_fetch_run`, converting
the upstream's flat response into the per-dayobs dict the cache requires.
Rows whose field is missing land under `None`, which the cache loop then
drops as out-of-range.

**`flatten_sorted(data, sort_field, descending=True)`**
(`utils/collation.py`) is the service-side inverse: it flattens a
per-dayobs dict back into one list sorted by a record field, with
records missing that field sorting as empty strings. Most log-style
services end in a one-line `collate_response` built on it.

**`flatten_within_dayobs(data, sort_field)`** (`utils/collation.py`) is
the same flattening for a field that only counts *within* a night, such
as `seq_num`: dayobs is the outer ordering and each night's records are
sorted inside it, where `flatten_sorted`'s single global sort would
interleave nights. `/exposures` and `/data-log` both use it, so the two
endpoints reading the one `consdb_exposures` entry return its rows in
the same order.

**`CachedAdapter._collate_runs(dayobs_list, fetch_run)`** is what turns
a set of cache-missing dayobs into as few range-based upstream calls as possible. It
seeds every requested dayobs with `_empty_value()` (an empty list by
default; override for a different shape), groups the dayobs into
contiguous runs with `contiguous_runs`, calls `fetch_run(start, end)`
once per run, and merges each run's partition back in — discarding any
dayobs outside the request, which upstreams sometimes return at range
boundaries. The seeding is what guarantees the "an entry for every
requested key" contract from [§4](#4-the-single-flight-cache-loop).

**`make_json_safe(obj)`** (`utils/serialization.py`) recursively
converts numpy scalars, pandas timestamps and `NaT`, astropy `Time`
objects, and non-finite floats into JSON-safe equivalents. Required for
anything derived from a DataFrame before it is returned from
`_fetch_from_source` — see
[§14](#14-serialisation-constraints-on-cached-values).

---

## 10. Worked example: a `/night-reports` request end to end

This is the simplest complete chain in the codebase: one service, one
adapter, no instrument dimension, no mixins. Take the request

```
GET /night-reports?dayObsStart=20250101&dayObsEnd=20250104
```

with `20250101` already cached and `20250102`–`20250103` not,

**1 · Route** — `main.read_night_reports(dayObsStart=20250101,
dayObsEnd=20250104, service=<NightReportService>)`.
`DayobsValidationMiddleware` has already rejected malformed or inverted
ranges, so the service can assume the pair is well-formed. FastAPI
resolves `service` through `Depends(get_night_report_service)`. The
route calls `service.handle_request(20250101, 20250104)`.

**2 · `Service.handle_request(*args)` → `dict`** — passes straight
through to `handle`, existing only to map failures: `HTTPException`
re-raised untouched, `requests.RequestException` → 502, anything else →
500, each logged with the service name
([§13](#13-failure-semantics)).

**3 · `NightReportService.handle(20250101, 20250104)` → `dict`** —
converts this endpoint's exclusive end bound to the inclusive bound the
adapter expects (`add_or_subtract_dayobs_days(day_obs_end, -1)` →
`20250103`; end-bound conventions differ per endpoint) and calls
`self.nightreport_adapter.fetch(20250101, 20250103)`.

**4 · `NightReportCachedAdapter`** delegates its `fetch` to
**`DayobsCachedAdapter.fetch(20250101, 20250103)` → `dict[int, list[dict]]`** 
— expands the inclusive range with `dayobs_range` into
`[20250101, 20250102, 20250103]` and hands it to `_fetch_cached`. This
is the boundary at which a range becomes a list of independent cache
keys.

**5 · `CachedAdapter._fetch_cached([20250101, 20250102, 20250103])` →
`dict[int, list[dict]]`** — probes
`adapter:nightreport:20250101` (hit, deserialised and kept),
`…:20250102` and `…:20250103` (miss). It wins the fetch lock
`lock:adapter:nightreport:20250102` and `…:20250103` via `SET NX EX`,
re-probes them, and calls `_fetch_from_source([20250102, 20250103])`.

**6 · `DayobsCachedAdapter._fetch_from_source([20250102, 20250103])` →
`dict[int, list[dict]]`** — delegates to
`_collate_runs(dayobs_list, self._fetch_run)`, which seeds both dayobs
with `[]`, and calls `contiguous_runs` — the two days are adjacent, so
this yields the single run `(20250102, 20250103)` and therefore one
upstream call rather than two.

**7 · `NightReportCachedAdapter._fetch_run(20250102, 20250103)` →
`dict[int, list[dict]]`** — the only method this adapter implements:

```python
reports = self._get_json_paged(
    f"{self.server}/nightreport/reports",
    params={
        "is_human": "either", "is_valid": "true", "order_by": "-day_obs",
        "min_day_obs": run_start,
        "max_day_obs": add_or_subtract_dayobs_days(run_end, 1),
    },
    page_limit=self._page_limit,
)
return self._partition_by_field(reports)
```

The `+1` on `max_day_obs` is the upstream API's exclusive-end
convention, unrelated to the endpoint's. `_get_json_paged` (from
`RestClient`) resolves `self.server`, retrieves a token, and issues
`limit`/`offset` pages until a short page arrives or the `MAX_RECORDS`
cap trips, returning one flat `list[dict]`. `_partition_by_field`
buckets those rows by their `day_obs` field into
`{20250102: [...], 20250103: [...]}`.

**8 · Back in `_collate_runs`** — each returned dayobs that is in the
requested set overwrites its seed; anything outside is dropped. Both
dayobs are present, so the result is the two-key dict.

**9 · Back in `_fetch_cached`** — each key is written with `_store`,
which serialises with `json.dumps(..., allow_nan=False)` and applies
`self._ttl(key)`. `_ttl` calls `_is_today(key)` — here
`dayobs == current_dayobs()` — to choose `TODAY_TTL_REDIS` for tonight
or `HISTORIC_TTL_REDIS` for a finished night. Both locks are released in
a `finally`, and the merged three-key dict returns up the stack.

**10 · `NightReportService.collate_response(per_day)` → `dict`** —
`{"reports": flatten_sorted(data, "day_obs")}`: one list, newest first.
This is the format the frontend expects for this endpoint.

**11 · Response** — FastAPI serialises the dict;
`CacheControlMiddleware` inspects `dayObsStart`/`dayObsEnd`, and because
the range does not include today's dayobs (and `/night-reports` is not a
mutable path) it sets `Cache-Control: public, max-age=86400`.

A second request for `20250101`–`20250104` skips from step 5 to step 
10: all three keys hit, and no lock, no `_fetch_run`, and no upstream HTTP
request occurs. The service collation still runs, allowing it to perform
tasks such as summing a value over the requested range.

---

## 11. Adding a new endpoint: what you must override

### The adapter

Create `adapters/<source>.py`.

**Required:**

1. **Choose a cache base** by key shape (table in
   [§2](#2-anatomy-of-an-adapter-cache-base--client--mixins)), plus a
   client if the source is REST or ConsDB SQL, plus any mixins —
   declared in order mixins-first, client, base last.
2. **Set `name`** to a unique, stable identifier. It namespaces the
   cache keys and log messages.
3. **Implement the fetch method:** `_fetch_run(run_start, run_end)` for
   `DayobsCachedAdapter`, `_fetch_run(instrument, run_start, run_end)`
   for `InstrumentDayobsCachedAdapter`, or `_fetch_from_source(ids)`
   for `IdCachedAdapter`. It must return JSON-serialisable data, and —
   for the ID base, which has no `_collate_runs` seeding — an entry for
   every requested key.
4. **Add a `@functools.cache` factory** `get_<source>_adapter()` that
   passes `get_redis_client()` and any composed adapters.
5. **Export the factory** from `adapters/__init__.py` (import and
   `__all__`), and add it to `REFRESH_ADAPTERS` in the same file if it
   is dayobs-keyed ([§12](#12-the-refresh-worker)).

**Optional, in rough order of how often it is needed:**

- `MutableDataMixin` if records can be edited after the fact.
- `__init__` for configuration (page size, limits, a composed adapter);
  call `super().__init__(redis, ...)` first.
- `_ttl` if neither standard policy fits (e.g., almanac).
- `auth_source` and/or a `server` property for a non-default upstream
  (e.g., Zephyr, Jira) —
  [§15](#15-upstream-authentication-and-server-resolution).
- `_empty_value` if a no-data key is not an empty list.
- `fetch` to validate arguments before they reach `_fetch_run` — call
  `super().fetch(...)` at the end (e.g., `ConsdbSqlMixin`).
- `_rows_from_result` for a `SqlClient` whose query joins tables with
  overlapping column names.

### The service

Create `services/<endpoint>.py`.

**Required:**

1. **Subclass `Service`.**
2. **`__init__`** with one named, typed, `None`-defaulting parameter per
   adapter, each falling back to its factory.
3. **`handle(...)`** with the endpoint's own typed signature. 
   This should : fetch, merge, return `self.collate_response(...)`. Use
   `fetch_concurrently` when there is more than one independent fetch.
4. **`collate_response(data)`** producing the response payload
   from the data returned by the adapters.
5. **A `@functools.cache` factory** `get_<endpoint>_service()`.
6. **Export the factory** from `services/__init__.py`.

### Wiring

7. **Register the route** in `main.py`: declare the query parameters,
   take `service=Depends(services.get_<endpoint>_service)`, and call
   `service.handle_request(...)`. Do not call `handle` directly — that
   bypasses the error mapping.
8. **Add the path to `_MUTABLE_PATHS`** in
   `middleware/cache_control.py` if the endpoint serves mutable data,
   matching the adapter's `MutableDataMixin`.
9. **Add the service to `WORKER_POOL_SERVICES`** in
   `services/__init__.py` if it mixes in `WorkerPoolMixin`
   ([§3](#3-anatomy-of-a-service)), so the application starts and stops
   its pool.

### Tests

The suite is split along the same lines as the code, and a new endpoint
should gain tests in all three places.

**`tests/adapters/`** — upstream parsing and caching behaviour. Use the
`fake_redis` fixture from `tests/conftest.py` for anything that touches
the cache: it expires entries for real and records the TTL used per
key, so TTL policy is directly assertable. Construct the adapter
directly with that fake client and an explicit `server_url`, and mock
the upstream at the transport boundary — `patch("requests.get")` (or
`requests.post` for `SqlClient` adapters) returning a `Mock` whose
`json()` yields the payload and whose `raise_for_status()` is a no-op.
Patching there rather than stubbing `_fetch_run` keeps the URL, the
query parameters, the paging, and the error handling under test; the
mock's `call_args_list` is what you assert against to prove, for
instance, that one contiguous run produced exactly one request. An
adapter whose source is not HTTP patches the library function it calls
instead (`get_dome_open_close`, `augment_visits`, and so on), by the
name it is bound to in the adapter module.

**`tests/services/`** — collation and response shape, with stub
adapters passed to the constructor. A service using `WorkerPoolMixin`
needs one addition: its tests patch the functions the service submits,
and a patched function cannot be pickled out to a worker, so those
modules replace `run_in_worker` with a fixture that calls through
in-process.

**`tests/test_endpoint_contracts.py`** — the route layer, which does
the same uniform job for every endpoint: declare its query parameters,
forward them to `service.handle_request`, and return the result
unchanged. These tests cover exactly that contract — parameter parsing
and coercion, the arguments actually forwarded, response pass-through,
422s for missing, malformed, or inverted dayobs, and `HTTPException`
status pass-through — by overriding the service dependency with a stub
or a capturing double. No service, adapter, or Redis is involved.

Adding an endpoint here is usually one entry in the `FORWARDING` list
(service getter, request URL, the argument tuple `handle_request`
should receive) plus its id in `FORWARDING_IDS`. That single entry is
parametrised into the pass-through, forwarding, and dayobs-validation
cases at once. Parameters that need more than positional forwarding —
booleans, repeated query parameters, optional flags — get their own
test alongside the existing exceptional cases.

---

## 12. The refresh worker

Today's entry is the hot key: every user looking at the current night
wants it, and it is the one entry that keeps changing. Left to demand,
it would expire every `TODAY_TTL_REDIS` and the next arrival would pay
the full upstream cost — repeatedly, 24/7. The refresh worker
keeps it warm so that in normal operation a request for today never
triggers an upstream fetch.

### The cycle

`RefreshWorker.run()` executes `_refresh_cycle` immediately — so a
deploy or restart warms the cache at once rather than after one idle
interval — and then repeats it every `interval_seconds` (default
`TODAY_TTL`, 5 minutes). Each subsequent cycle starts one interval after
the *previous cycle started*: the elapsed run time is deducted from the
wait, and a cycle that overruns its interval is followed immediately by
the next.

Each cycle calls `adapter.refresh(today)` on every registered adapter,
in registration order. `refresh` is **fetch-then-overwrite**: it fetches
fresh data first and only then replaces the entry with a single `SET`.
The old value is never deleted ahead of the fetch, so a request arriving
mid-refresh is served the previous value instead of falling into a
cold-miss window, and a failed fetch leaves the existing entry intact.
This is also why `refresh` bypasses the single-flight lock — it never
empties the cache, so there is no stampede to prevent, and the worst
case against a racing cold fetch is one redundant upstream call.
`InstrumentDayobsCachedAdapter.refresh` does this once per instrument,
catching per instrument so one instrument's upstream failure does not
stop the others.

**`INSTRUMENTS`.** A refresh has no request to take an instrument from,
so the set of instruments to warm is a class constant on
`InstrumentDayobsCachedAdapter` — currently `("lsstcam", "latiss")`.
The same constant is what `ConsdbSqlMixin._validate_instrument` checks
a request against, so it carries two jobs at once: adding an instrument
there both starts warming its entries every cycle and makes it an
accepted value on the endpoints, while removing one stops the warming
and turns previously valid requests into 422s. The values are
lower-case because they are used verbatim in cache keys
(`adapter:consdb_exposures:lsstcam:20250101`) and interpolated into
ConsDB schema names (`cdb_lsstcam.exposure`); `fetch` lower-cases the
incoming instrument before either use. Overriding the constant on a
single adapter is legitimate if one source serves a different set.

**Rollover.** When the astronomical dayobs rolls over at 12:00 UTC, the
worker refreshes the *previous* dayobs one final time before moving on.
This finalisation pass fetches the now-complete night and re-stores it
with the long historic TTL. Without it, yesterday's entry would still
carry the short today TTL and expire minutes after rollover — a
guaranteed daily cold miss on the most-viewed historical night, possibly
serving a copy truncated at the last pre-rollover refresh.

**Failure isolation.** `_refresh_all` catches per adapter: one adapter's
failure is logged with its `name` and the loop continues.
`_refresh_cycle` itself never raises, so the loop cannot die; a cycle
that fails wholesale is logged and retried at the next interval. Each
cycle logs its duration and success/failure counts, and warns if it
consumed more than `SLOW_CYCLE_FRACTION` (50%) of the interval — the
signal that the interval is too short for the work, or that an upstream
has degraded.

`stop()` ends the `RefreshWorker` gracefully, allowing a currently
in-progress adapter fetch to complete before ending, or ending
immediately if a cycle is not in-progress.

### Registering an adapter

Add its factory to `REFRESH_ADAPTERS` in `adapters/__init__.py`, beside
`__all__`:

```python
REFRESH_ADAPTERS = (
    get_consdb_exposures_adapter,
    # After consdb_exposures: the overhead adapter reads that cache,
    # so a cycle warms it from the freshly-refreshed exposures.
    get_visit_overhead_adapter,
    ...
)
```

The list lives with the adapters rather than in the entrypoint because
what is worth keeping warm is a property of the adapter, not of the
process that starts the worker — and because adding an adapter then puts
both lists in front of you. `run_refresh_worker.py` only calls each
factory and hands the results to `RefreshWorker`.

Order matters only for composed adapters — put a dependent adapter after
the adapter it reads, which is why the tuple is not sorted to match
`__all__`. Membership is not simply "every dayobs-cached adapter":
`IdCachedAdapter` implementations (zephyr, jira_block) are **not**
registered, since they have no "today" entry and no `refresh` method and
their keys are only known from a request, and `almanac` is not either,
being local computation cheap enough that warming it buys nothing.
`tests/test_refresh_worker.py` asserts both exclusions.

### In infrastructure

The worker runs as its own process, separate from the API service, both
built from the same image and sharing one Redis:

- `pyproject.toml` declares the console script
  `run_refresh_worker = lsst.ts.logging_and_reporting.run_refresh_worker:run_refresh_worker`.
- `docker/refresh_worker.sh` sources the environment and runs it;
  `docker/startup.sh` is the API container's equivalent. The Dockerfile
  defaults to `docker/startup.sh`, so the worker container overrides the
  command with `docker/refresh_worker.sh`.
- `run_refresh_worker()` installs SIGTERM and SIGINT handlers that call
  `worker.stop()`, so container shutdown lets the refresh in progress
  finish instead of killing it mid-fetch.
- If `ND_CACHING_DISABLE_REDIS` is set the worker logs a warning and
  exits immediately: with nothing to warm it would only add upstream
  load.

**Exactly one instance must run per deployment.** Nothing in the process
coordinates with peers, so a second instance merely duplicates upstream
fetches (harmlessly — fetch-then-overwrite is idempotent — but
wastefully). Uniqueness is the deployment's responsibility: a single
`refresh-worker` container in docker-compose, a single-replica
deployment in Kubernetes.

---

## 13. Failure semantics

Three rules, applied at three different levels.

**Adapters are all-or-nothing.** Any upstream error inside
`_fetch_from_source` propagates out of `_fetch_cached` and fails the
whole call. Nothing partial is stored, so a cached entry is always a
complete answer for its key. Adapters do catch errors that are *not*
failures — `ZephyrAdapter` maps a 404 to a cached `None` so an unknown
BLOCK key does not re-query upstream on every request.

**`Service.handle_request` maps exceptions to HTTP responses.**
`HTTPException` (raised deliberately — e.g. `ConsdbSqlMixin`'s 422 for
an unknown instrument, or `WorkerPoolMixin`'s 503 when its pool is
saturated) passes through untouched;
`requests.RequestException` becomes a 502 naming the service; anything
else becomes a 500. All are logged with a traceback, while
a minimal error message is returned to the user. Endpoints
**must** call `handle_request`, never `handle` to gain these benefits.

`redis.RedisError` is caught separately, purely so it can be logged at
`CRITICAL` (`doc/logging.md`) — an unreachable cache is a deployment
fault, not a bad request. The response is the same 500 the generic
branch produces.

**Services decide what is essential.** With `fetch_concurrently`
returning exceptions as values, a service can fail the request on a core
source and degrade on a peripheral one. `ExposuresService` re-raises a
ConsDB failure but reports dome and time-accounting failures as
`open_dome_error` and `time_accounting_error` fields alongside `None`
data. `BlockDetailsService` populates a per-source `errors` map and
fails only when both sources fail. When you add a service, decide
explicitly which of its adapters are load-bearing — the default of
letting everything fail the request is right for a single-adapter
service and usually wrong for a multi-adapter one.

---

## 14. Serialisation constraints on cached values

Cache entries are JSON documents. Whatever `_fetch_from_source` returns
is passed to:

```python
json.dumps(data, allow_nan=False, ensure_ascii=False, separators=(",", ":"))
```

Three consequences:

- **`allow_nan=False`.** `NaN`, `Infinity`, and `-Infinity` are not
  valid JSON, and Python's default of emitting them anyway produces
  documents that browsers and other consumers reject. A non-finite float
  raises `ValueError` at store time instead — loudly, at the adapter
  that produced it.
- **Only JSON types survive.** numpy scalars, pandas `Timestamp`/`NaT`,
  astropy `Time`, and DataFrames are not serialisable. Any adapter whose
  `_fetch_run` goes through pandas or numpy must pass its rows through
  `make_json_safe` before returning them, as `VisitOverheadAdapter`
  does. `make_json_safe` converts numpy scalars to Python types,
  timestamps to ISO strings, `NaT`/`NA` to `None`, and non-finite floats
  to `None` — which is also what keeps `allow_nan=False` from firing.
- **What goes in is what comes out.** A cache hit returns
  `json.loads` of the stored bytes, so tuples come back as lists and
  integer-keyed dicts come back with string keys. Keep cached values to
  lists of flat dicts, or to a scalar, and let the service do any
  reshaping.

`ensure_ascii=False` and the compact separators only reduce the stored
byte count; they have no effect on the decoded value.

---

## 15. Upstream authentication and server resolution

Everything here lives in `utils/auth.py` and is consumed by the
transport clients in [§2](#2-anatomy-of-an-adapter-cache-base--client--mixins),
so an adapter normally inherits all of it and sets at most one
attribute.

### Which server an adapter talks to

`RestClient.server` returns the `server_url` passed to the constructor
if there is one, and otherwise `Server.get_url()` — the deployment's
own base URL, read from the `EXTERNAL_INSTANCE_URL` environment
variable. That variable must match one of five known deployments
exactly:

| Deployment | `EXTERNAL_INSTANCE_URL` |
|---|---|
| Summit | `https://summit-lsp.lsst.codes` |
| Base | `https://base-lsp.lsst.codes` |
| Tucson test stand | `https://tucson-teststand.lsst.codes` |
| USDF | `https://usdf-rsp.slac.stanford.edu` |
| USDF dev | `https://usdf-rsp-dev.slac.stanford.edu` |

Anything else — including unset — makes `Server.get_url()` raise
`ValueError`, which surfaces as a 500 from the endpoint that triggered
the fetch. 

Adapters whose upstream is *not* the local deployment override the
`server` property instead: `ZephyrAdapter` returns the Zephyr Scale
cloud API, and `JiraApiMixin` builds `https://{JIRA_API_HOSTNAME}`
(raising a 500 if that variable is unset). The `server_url` constructor
argument exists mainly so tests can point an adapter at a fake host.

### Which token an adapter sends

`AUTH_SOURCES` holds one entry per credential, and an adapter selects
one with the `auth_source` class attribute:

| `auth_source` | Environment variable | Used by |
|---|---|---|
| `"rsp"` (default) | `ACCESS_TOKEN` | everything on the local deployment — nightreport, exposurelog, narrativelog, ConsDB, `rubin_nights` |
| `"jira"` | `JIRA_API_TOKEN` | `JiraApiMixin` adapters (`jira_obs`, `jira_block`) |
| `"zephyr"` | `ZEPHYR_API_TOKEN` | `ZephyrAdapter` |

`retrieve_access_token(config)` reads the environment variable the
source names and returns it. The backend authenticates as a
**service account**, so there is no per-user token,
and a caller's own `Authorization` header is never consulted.
An unset variable is a misconfigured deployment rather than a
bad request, so it raises a **500** — logged naming the source,
returned to the client as a bare "Server configuration error". The failure 
surfaces on the first fetch that needs the token, not at startup.

The lookup runs per fetch rather than being cached on the adapter, so
a rotated token takes effect without a restart. `RubinNightsClientsMixin` 
is the exception: it resolves the RSP token once, when it builds its clients
([§7](#7-dependency-injection)), so rotating that token does need a
restart before the EFD and context-feed adapters pick it up.

Header construction differs by source: `RestClient._request_headers`
uses `get_auth_header`, a bearer header, while `JiraApiMixin`
overrides it to send `Authorization: Basic <token>` plus a JSON
content type.

### Adding an adapter for a new upstream

If the source lives on the deployment and accepts the RSP token,
subclass `RestClient` and set nothing — the defaults are correct. If it
is external, override `server`, and add an entry to `AUTH_SOURCES` —
an `env_var` naming its variable and a `label` for the error log —
then point `auth_source` at it. Override
`_request_headers` only if the upstream wants something other than a
bearer token.

---

## Appendix: environment variables

| Variable | Default | Read by | Purpose |
|---|---|---|---|
| `EXTERNAL_INSTANCE_URL` | *(none — required)* | `utils/auth.py` | Identifies the deployment and supplies the default upstream base URL. Must exactly match a known deployment URL ([§15](#15-upstream-authentication-and-server-resolution)); otherwise every adapter fetch fails. |
| `ACCESS_TOKEN` | *(none — required)* | `utils/auth.py` | RSP service-account token for deployment-local APIs and the `rubin_nights` clients. Unset means every fetch that needs it fails with a 500. |
| `JIRA_API_TOKEN` | *(none)* | `utils/auth.py` | Jira credential, sent as Basic auth. |
| `JIRA_API_HOSTNAME` | *(none)* | `utils/auth.py` | Jira host; also used to build the BLOCK links in `/block-details` responses. |
| `ZEPHYR_API_TOKEN` | *(none)* | `utils/auth.py` | Zephyr Scale credential. |
| `REDIS_HOST` | `localhost` | `redis_client.py` | Redis hostname (`redis` in the dev compose stack). |
| `REDIS_PORT` | `6379` | `redis_client.py` | Redis port. |
| `REDIS_DB` | `0` | `redis_client.py` | Redis logical database number. |
| ND_CACHING_DISABLE_NGINX | unset | `frontend: docker/nginx.conf.template` | Any value other than empty or 0 disables the nginx cache |
| `ND_CACHING_DISABLE_REDIS` | unset | `redis_client.py` | Any value other than empty or `0` disables caching entirely ([§5](#5-cache-keys-and-the-redis-client)) and makes the refresh worker exit at startup. |
| `LOG_LEVEL` | `INFO` | `utils/logging_config.py` | Log level for the entire app. |

The API service needs all of these to be set correctly; the refresh worker 
needs the same set, since it drives the same adapters.
