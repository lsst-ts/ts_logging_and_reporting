# Logging

This document describes how the backend logs: how the logger is
configured, how trace_id correlates responses and what each layer
logs at what level.


---

## 1. One configuration, three kinds of process

Everything here lives in `utils/logging_config.py`. It is a module
rather than a couple of lines in each entrypoint because three
different kinds of process need identical configuration:

| Process | Configured by |
|---|---|
| The API | `main.py`, at import |
| The refresh worker | `run_refresh_worker.py`, at import |
| Each WorkerPoolMixin worker | `WorkerPoolMixin`'s pool initialiser |

This exposes a `logging_config.configure_logging` that should be run
at process startup. Every module then uses `logging.getLogger(__name__)`
to get its own `logger` which records the name of the module logging
each record.

---

## 2. Level

The `LOG_LEVEL` environmental variable sets the level for the application
and uvicorn. `logging_configlog_level()` is a helper to ensure this is
a useful value.

`LOG_LEVEL` must match uvicorn's accepted set: `CRITICAL`, `ERROR`,
`WARNING`, `INFO`, `DEBUG` - values outside this range will warn on
server startup, and default to `INFO`


---

## 3. Trace IDs

Records include a trace_id are formatted as:

```
LEVEL [logger name] [trace_id] message
```

`TraceIdFilter` is attached to the root *handler* rather than to any
logger, so it stamps every record that reaches it whatever emitted it,
including records from libraries, and an ordinary `logger.debug(...)`
call anywhere in the tree carries one without doing anything. A record
logged outside a traced unit of work shows `-` (`NO_TRACE_ID`) instead.

### What is a "traced unit of work"?

A traced unit of work is either a request or a run of the RefreshWorker

- `RequestLoggingMiddleware` sets one per request, so all Services,
  Adapters, and any other logging associated with that request shares
  a tag
- `RefreshWorker._refresh_cycle` sets one per cycle, so the cycle's own
  lines and everything its adapters log underneath it share a tag.

### Crossing concurrency boundaries

The ID is a `ContextVar`, which crosses neither concurrency boundary this
application uses, so both are bridged explicitly:

| Boundary | How it crosses |
|---|---|
| `Service.fetch_concurrently` → thread | Submits `contextvars.copy_context().run` rather than the thunk itself — a copy per task, since one `Context` cannot be entered by two threads at once. |
| `WorkerPoolMixin.run_in_worker` → process | Context does not survive pickling, so the ID is passed as a call argument and re-established inside the worker. |

Anything else that hands work to a thread or a process has to do the
same, or its records will be attributed to no trace_id.

---

## 4. What each layer logs

| Layer | Info | Debug |
|---|---|---|
| `RequestLoggingMiddleware` | arrival, with query string | completion, with status and duration |
| `CachedAdapter` | — | verbose cache internals |
| `RestClient` | — | every GET and POST, with status and duration |
| Services | one completion line per request, with counts | — |
| `RefreshWorker` | cycle start, cycle duration with success/failure counts, dayobs rollover | — |
| `WorkerPoolMixin` | pool startup | submission and call duration |

The cache and transport layers are the noisy ones, and both are entirely
debug: in normal operation they are silent, and turning `LOG_LEVEL` down
to `DEBUG` is what answers "was this served from Redis or fetched?" and
"which upstream is slow?". 


### Durations that warn on their own

Four thresholds escalate to a warning regardless of level, so a
degradation is visible without anyone having enabled debug first:

| Threshold | Where | What | Default |
|---|---|---|---|
| `SLOW_REQUEST_SECONDS` | `middleware/request_logging.py` | Time in a request | 10 s |
| `RestClient.SLOW_REQUEST_SECONDS` | `adapters/base_clients.py` | Time to complete an upstream request | 10 s |
| `WorkerPoolMixin.SLOW_CALL_SECONDS` | `services/worker_pool_mixin.py` | Time for a worker to complete | 30 s |
| `SLOW_CYCLE_FRACTION` of the interval | `refresh_worker.py` | Portion of the RefreshWorker interval taken for one cycle | 50% |


### Request logging is middleware, and outermost

Logging requests in one middleware rather than in each endpoint covers
the requests that never reach a service — FastAPI's 422 for a bad
parameter, or a rejection from `DayobsValidationMiddleware`.

`RequestLoggingMiddleware` is also what establishes the trace ID for a
request ([§3](#3-trace-ids)): it calls `set_trace_id` before `call_next`,
so every record logged while serving the request — by the other
middlewares, the service, its adapters, and any thread or worker they
hand work to — carries the same ID. It is not merely the component that
logs two lines; it is the one that makes every other line attributable.

Both jobs want it outermost, and it is added last in `main.py` to get
there: its timing covers dayobs validation, CORS and cache-control rather
than just the handler, and any record those middlewares emit is inside
the traced region.

`/health` is explicitly not logged by neither request line: the Kubernetes
readiness and liveness probes hit it often enough to drown everything else.

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

---

## 6. Reading the logs

Filter one request or one refresh cycle out of an interleaved log by its
trace ID. The arrival line gives you both the ID and the query:

```
INFO [.....middleware.request_logging] [4f2a91c3] Fetching /exposures with dayObsStart=20250101&dayObsEnd=20250108&instrument=lsstcam
```

Grepping or otherwise searching by `[4f2a91c3]` here would show you only the relevant logs for this query.


