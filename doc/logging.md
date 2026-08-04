# Logging

This document describes how the backend logs: where the configuration
lives and why it is shared, what `LOG_LEVEL` does, how a trace ID gets
onto every record, what each layer logs at which level, and what
`CRITICAL` is reserved for.

For the layering it describes — services, adapters, the cache and the
refresh worker — see `doc/service-adapter-infrastructure.md`.

---

## 1. One configuration, three kinds of process

Everything here lives in `utils/logging_config.py`. It is a module
rather than a couple of lines in each entrypoint because three
different kinds of process need identical configuration:

| Process | Configured by |
|---|---|
| The API | `main.py`, at import |
| The refresh worker | `run_refresh_worker.py`, at import |
| Each map-render worker | `WorkerPoolMixin`'s pool initialiser |

The third is what forces the issue. Pool workers are forked from the
forkserver, which imports only the preload modules and never runs an
entrypoint, so a worker that does not configure logging itself sends its
debug and info records nowhere at all, and its warnings fall through to
`logging.lastResort` without the application's format.

Every module takes `logging.getLogger(__name__)`. Nothing sets a level
on its own logger, and nothing logs through `uvicorn.error`.

---

## 2. Level

`LOG_LEVEL` sets the level for the application *and* for uvicorn, so the
access and error logs move with everything else rather than sitting at a
fixed level of their own. `log_level()` is the single reader for both.

It validates rather than passing the value through: unset, blank, or a
name outside `LOG_LEVELS` falls back to `INFO`. Both consumers fail hard
on a bad value — docker compose expands an unset variable to an empty
string, which `basicConfig` rejects outright, and a level name uvicorn
does not recognise raises `KeyError` and takes the server down at
startup — so the fallback is worth more than the strictness. A value that
*was* set but is not understood is warned about once logging is up, so
the fallback is not silent.

`LOG_LEVELS` matches uvicorn's accepted set: `CRITICAL`, `ERROR`,
`WARNING`, `INFO`, `DEBUG`.

---

## 3. Trace IDs

Records are formatted as:

```
LEVEL [logger name] [trace id] message
```

`TraceIdFilter` is attached to the root *handler* rather than to any
logger, so it stamps every record that reaches it whatever emitted it,
including records from libraries, and an ordinary `logger.debug(...)`
call anywhere in the tree carries one without doing anything. A record
logged outside a traced unit of work shows `-` (`NO_TRACE_ID`).

### What a "traced unit of work" is

A request or a refresh cycle — which is why it is a trace ID and not a
request ID:

- `RequestLoggingMiddleware` sets one per request, before `call_next`,
  so the downstream task copies a context that already has it.
- `RefreshWorker._refresh_cycle` sets one per cycle, so the cycle's own
  lines and everything its adapters log underneath it share a tag. The
  worker clears it before the stopped line, which belongs to no cycle.

### Crossing concurrency boundaries

The ID is a `ContextVar`, which crosses neither concurrency boundary this
application uses, so both are bridged explicitly:

| Boundary | How it crosses |
|---|---|
| `Service.fetch_concurrently` → thread | Submits `contextvars.copy_context().run` rather than the thunk itself — a copy per task, since one `Context` cannot be entered by two threads at once. |
| `WorkerPoolMixin.run_in_worker` → process | Context does not survive pickling, so the ID is passed as a call argument and re-established inside the worker. |

Anything else that hands work to a thread or a process has to do the
same, or its records will be attributed to no request.

---

## 4. What each layer logs

| Layer | Info | Debug |
|---|---|---|
| `RequestLoggingMiddleware` | arrival, with the query string | completion, with status and duration |
| `CachedAdapter` | — | per-key hit/miss with entry size, store with size and TTL, per-fetch cache-hit summary, time spent waiting on another request's single-flight lock |
| `RestClient` | — | every GET and POST, with status and duration |
| Services | one completion line per request, with counts | — |
| `RefreshWorker` | cycle start, cycle duration with success/failure counts, dayobs rollover | — |
| `WorkerPoolMixin` | pool startup | submission and call duration |

The cache and transport layers are the noisy ones, and both are entirely
debug: in normal operation they are silent, and turning `LOG_LEVEL` down
to `DEBUG` is what answers "was this served from Redis or fetched?" and
"which upstream is slow?". Their debug calls are guarded with
`logger.isEnabledFor(logging.DEBUG)`, so neither the f-strings nor the
payload summaries are built when debug is off.

`/health` is logged by neither request line: the Kubernetes readiness and
liveness probes hit it often enough to drown everything else.

### Durations that warn on their own

Four thresholds escalate to a warning regardless of level, so a
degradation is visible without anyone having enabled debug first:

| Threshold | Where | Default |
|---|---|---|
| `SLOW_REQUEST_SECONDS` | `middleware/request_logging.py` | 10 s |
| `RestClient.SLOW_REQUEST_SECONDS` | `adapters/base_clients.py` | 10 s |
| `WorkerPoolMixin.SLOW_CALL_SECONDS` | `services/worker_pool_mixin.py` | 30 s |
| `SLOW_CYCLE_FRACTION` of the interval | `refresh_worker.py` | 50% |

The slow-request line repeats the query string, so a slow request is
actionable from that one line without going back for the arrival line.

### Request logging is middleware, and outermost

Logging requests in one middleware rather than in each endpoint covers
the requests that never reach a service — FastAPI's 422 for a bad
parameter, or a rejection from `DayobsValidationMiddleware` — which a
handler is by definition not running to log.

`RequestLoggingMiddleware` is also what establishes the trace ID for a
request ([§3](#3-trace-ids)): it calls `set_trace_id` before `call_next`,
so every record logged while serving the request — by the other
middlewares, the service, its adapters, and any thread or worker they
hand work to — carries the same ID. It is not merely the component that
logs two lines; it is the one that makes every other line attributable.

Both jobs want it outermost, and it is added last in `main.py` to get
there: its timing covers dayobs validation, CORS and cache-control rather
than just the handler, and any record those middlewares emit is inside
the traced region. The corollary is that a request it skips is a request
with no trace ID, which is why the skip list is only `/health`.

---

## 5. CRITICAL means the deployment is wrong

`CRITICAL` is reserved for faults no request can recover from, that need
someone to change the environment:

| Case | Raised as |
|---|---|
| Service-account token unset (`ACCESS_TOKEN`, `JIRA_API_TOKEN`, `ZEPHYR_API_TOKEN`) | 500 `Server configuration error` |
| `JIRA_API_HOSTNAME` unset | 500 |
| `EXTERNAL_INSTANCE_URL` unset or not a known deployment | 500 |
| Redis unreachable (`redis.RedisError`) | 500 |

Keeping a level for these means they stay visible at whatever level a
deployment runs at, and are not mixed in with the ordinary upstream
failures that `logger.exception` reports at error.

None of the four changes what the client sees. In particular
`Service.handle_request` catches `redis.RedisError` ahead of its generic
handler purely so it can be logged this way; the status is the same 500
the generic branch produces, and the detail is unchanged.

---

## 6. Reading the logs

Filter one request or one refresh cycle out of an interleaved log by its
trace ID. The arrival line gives you both the ID and the query:

```
INFO [.....middleware.request_logging] [4f2a91c3] Fetching /exposures with dayObsStart=20250101&dayObsEnd=20250108&instrument=lsstcam
```

```
grep '\[4f2a91c3\]' backend.log
```

That will include the records from any thread `fetch_concurrently`
started and any map render that ran in a worker process, since both carry
the ID across ([§3](#3-trace-ids)).

To see cache and upstream behaviour for a request, run the process with
`LOG_LEVEL=DEBUG` and filter the same way — the hit/miss lines name the
full cache key, so they line up with what `redis-cli --scan` shows.
