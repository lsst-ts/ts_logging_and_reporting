# EFD Queries from the Nightly Digest

This document traces EFD calls from the adapter entry points in this repository through nested `rubin_nights` helpers to the client methods that issue the queries. It uses the `where-efd.md` outline; that original file is unchanged.

The adapter source is in this repository. The nested `rubin_nights` implementations described below are from the package installed in the active `logrep` environment (`/opt/anaconda3/envs/logrep/lib/python3.13/site-packages/rubin_nights/`). Exact behavior may differ in another deployment if it uses a different package build.

## How EFD client methods become queries

The `InfluxQueryClient` used by `rubin_nights` translates:

- `select_time_series(topic, fields, t_start, t_end, index=...)` into a time-bounded `SELECT` from that topic. The time bounds are inclusive. A non-`None` index adds a `salIndex` filter; `index=None` leaves the query unfiltered by SAL index.
- `select_top_n(topic, fields, num=1, time_cut=..., index=...)` into a query at or before the time cut, optionally filtered by `salIndex`, grouped by all tags, ordered descending, and limited to `num` rows.
- `query(sql)` sends the raw query string directly.

The client checks topic availability using `get_topics()` before building topic-based selections. The query methods ultimately send the InfluxQL query through the EFD client's query endpoint. Thus, the methods and query details below identify the data queries; metadata/topic discovery can also occur.

## Rubin Nights Context Adapter

Entry point: [`RubinNightsContextAdapter._fetch_run`](python/lsst/ts/logging_and_reporting/adapters/rubin_nights_context.py#L81) calls `rubin_nights.scriptqueue.get_consolidated_messages(t_start, t_end, clients, all_tracebacks=True)`. The adapter's window starts at the requested dayobs and ends at the day after `run_end` plus six hours. `get_consolidated_messages` receives `fetch_errors=True` by default, and the adapter explicitly enables all tracebacks.

Call outline:

```text
RubinNightsContextAdapter._fetch_run
└── get_consolidated_messages
    ├── get_script_status
    │   ├── get_script_stream
    │   └── get_script_state
    ├── get_scriptqueue_tracebacks
    ├── get_scheduler_configs
    ├── get_narrative_and_errors
    │   ├── narrative_log_client.query_log                  (not EFD)
    │   ├── EFD observatory-status selection
    │   ├── get_error_codes                                  (dynamic EFD topics)
    │   └── get_all_tracebacks                               (dynamic EFD topics)
    ├── get_exposure_info
    │   ├── EFD camera selections
    │   └── exposure_log_client.query_log                   (not EFD)
    └── EFD command_addBlock selection
```

### Script status and stream

`get_script_status` first selects `lsst.sal.ScriptQueue.logevent_summaryState` with fields `salIndex`, `summaryState`, over the requested interval.

- If that query contains no `ENABLED` events, the helper makes one set of stream/state queries: `get_script_stream` queries the two topics below, and `get_script_state` selects the state topic with `index=None` (all queues).
- If `ENABLED` events exist, it queries `summaryState` again for each queue index, splits the interval around queue restarts, and queries the stream and state for each resulting interval. State selections use that queue's `salIndex`; stream selections have no queue-index filter. The number of queries therefore depends on EFD data and restart intervals.

`get_script_stream` makes these `select_time_series` calls:

- Topic `lsst.sal.Script.logevent_description`; fields `classname`, `description`, `salIndex`.
- Topic `lsst.sal.Script.command_configure`; fields `blockId`, `config`, ` executionId`, `salIndex`. The leading space in ` executionId` is present in the installed source.

`get_script_state` selects `lsst.sal.ScriptQueue.logevent_script` with fields `blockId`, `path`, `processState`, `scriptState`, `salIndex`, `scriptSalIndex`, `timestampProcessStart`, `timestampConfigureStart`, `timestampConfigureEnd`, `timestampRunStart`, and `timestampProcessEnd`. It uses the requested time interval and, in the restart branch, the queue's `salIndex`.

### Scheduler configuration

`get_scheduler_configs` defaults to queue indices 1 and 2. For each queue, the EFD client makes both a `select_top_n(..., num=1, time_cut=..., index=queue)` and a `select_time_series(..., index=queue)` selection for each topic:

- `lsst.sal.Scheduler.logevent_configurationApplied`; fields `SchedulerId`, `configurations`, `salIndex`, `schemaVersion`, `url`, `version`. The top-one lookup is at `t_start`; the series covers `t_start` through `t_end`.
- `lsst.sal.Scheduler.logevent_dependenciesVersions`; fields `cloudModel`, `downtimeModel`, `seeingModel`, `skybrightnessModel`, `observatoryLocation`, `observatoryModel`, `scheduler`, `salIndex`, `version`. The top-one lookup uses the first configuration timestamp as its time cut; the series covers `t_start` through `t_end`.

The helper also queries `lsst.obsenv.summary` through `obsenv_client`, not the standard EFD client. It uses fields `summit_extras`, `summit_utils`, `ts_standardscripts`, `ts_externalscripts`, `ts_config_ocs`, with one `select_top_n` at the first scheduler-configuration timestamp and one `select_time_series` through `t_end`. This is an obsenv database query and is listed to distinguish it from standard EFD calls.

### Tracebacks and error codes

`get_scriptqueue_tracebacks` always sends one raw `efd_client.query` for `message`, `traceback`, and `salIndex` from `lsst.sal.Script.logevent_logMessage`, restricted to the requested interval and non-empty tracebacks. The source builds it from these fragments:

```python
query = 'select message, traceback, salIndex from "lsst.sal.Script.logevent_logMessage"'
query += f"where time >= '{t_start.isot}Z' and time <= '{t_end.isot}Z' and traceback != ''"
```

Because `all_tracebacks=True`, `get_all_tracebacks` also calls `get_topics()`, keeps every topic containing `logMessage` except `lsst.sal.Script.logevent_logMessage`, and sends one raw `query` per matching topic. Each query selects `*` and filters the requested time interval and `traceback != ''`. The topic set is determined at runtime by the connected EFD.

Because `fetch_errors` defaults to `True`, `get_error_codes` calls `get_topics()`, keeps every topic containing `errorCode`, then calls `select_time_series` for each with fields `errorCode`, `errorReport` over the requested interval. The number and names of those queries depend on the connected EFD.

### Narrative, observatory status, and exposures

`get_narrative_and_errors` calls `narrative_log_client.query_log(t_start, t_end)`; that is a narrative-log query, not an EFD call. It separately selects `lsst.sal.Scheduler.logevent_observatoryStatus` from EFD with fields `status`, `note`, `statusLabels`. This duplicates the topic queried by the dedicated observatory-status adapter below.

`get_exposure_info` selects each of the following EFD topics with fields `imageName`, `imageIndex`, `exposureTime`, `darkTime`, `measuredShutterOpenTime`, `additionalValues`, `timestampAcquisitionStart`, `timestampDateEnd`, `timestampDateObs`:

- `lsst.sal.MTCamera.logevent_endOfImageTelemetry`
- `lsst.sal.CCCamera.logevent_endOfImageTelemetry`
- `lsst.sal.ATCamera.logevent_endOfImageTelemetry`

These are three `select_time_series` calls over the requested interval. The helper also calls `exposure_log_client.query_log(t_start, t_end)` to join exposure-log records; that is not an EFD call.

### Block names

At the end of `get_consolidated_messages`, the EFD client selects `lsst.sal.Scheduler.command_addBlock` with fields `id`, `salIndex` over the requested interval and `index=None`. This includes all SAL indices.

## Rubin Nights Dome Adapter

Entry point: [`RubinNightsDomeAdapter._fetch_run`](python/lsst/ts/logging_and_reporting/adapters/rubin_nights_dome.py#L51) calls `rubin_nights.observatory_status.get_dome_open_close(t_start, t_end, efd_client)`. The helper issues two direct `efd_client.query` calls against `lsst.sal.MTDome.apertureShutter`. Both select `positionActual0`, `positionActual1`, `positionCommanded0`, and `positionCommanded1`, with the adapter's time bounds:

- Open events: both absolute commanded positions equal 100; both absolute actual positions are between 25 and 85 inclusive.
- Close events: both absolute commanded positions equal 0; both absolute actual positions are between 25 and 85 inclusive.

The dome helper's default sunset/sunrise calculations do not issue additional EFD queries. The adapter's end bound is noon UTC on the day after `run_end` to cover the dayobs interval.

## Rubin Nights Observatory Status Adapter
@@ ## Rubin Nights Observatory Status Adapter

Entry point: [`RubinNightsObsStatusAdapter._fetch_run`](python/lsst/ts/logging_and_reporting/adapters/rubin_nights_obs_status.py#L54) directly calls `select_time_series` on `lsst.sal.Scheduler.logevent_observatoryStatus` with fields `status`, `note`, `statusLabels`, over the adapter's dayobs window. No `salIndex` filter is supplied. The same topic is queried separately by the context adapter's `get_narrative_and_errors` path.

## Visit Overhead Adapter
@@ ## Visit Overhead Adapter

Entry point: [`VisitOverheadAdapter._fetch_run`](python/lsst/ts/logging_and_reporting/adapters/visit_overhead.py#L74) reads exposures from the ConsDB exposures adapter's cache, calls `rubin_nights.augment_visits.augment_visits`, then passes the visits and EFD client to `rubin_nights.rubin_scheduler_addons.add_model_slew_times`.

If there are no exposure rows, `_fetch_run` returns before the slew model and makes no EFD calls. For non-empty visits, the slew model obtains mount limits through `get_tma_limits`. That helper makes four EFD selections, with no `salIndex` filter:

- `lsst.sal.MTMount.logevent_elevationControllerSettings`; fields `minL1Limit`, `maxL1Limit`, `maxMoveVelocity`, `maxMoveAcceleration`, `maxMoveJerk`.
- `lsst.sal.MTMount.logevent_azimuthControllerSettings`; same five fields.

For each topic it calls `select_top_n(..., num=1, time_cut=t_start)` to seed the settings at the start of the interval, then `select_time_series(t_start, t_end)` for updates during the interval. These slew-model EFD calls are conditional on having visits to model.

The source exposures themselves are fetched by the composed ConsDB exposures adapter, not by EFD. That adapter always queries ConsDB; on USDF/USDF-dev it conditionally adds a left join to `efd_{instrument}.exposure_efd` when fields are configured. The only configured transformed-EFD field is `mt_salindex112_temperature_0_mean` for `lsstcam`; `latiss` has no transformed-EFD join. This is SQL through the ConsDB client, not a direct `efd_client` call.

## What Exactly Is Called Every 5 Minutes
@@ ## What Exactly Is Called Every 5 Minutes

### RefreshWorker

`run_refresh_worker` starts the background worker only when Redis caching is enabled. The worker's interval defaults to `TODAY_TTL_CLIENT`, 300 seconds. `RefreshWorker.run` runs its first cycle immediately, then waits only the remaining interval after each cycle; an overlong cycle is followed immediately by another cycle.

Each normal cycle obtains today's dayobs and calls `_refresh_all(today)`. When the dayobs rolls over, it first refreshes the previous dayobs once, then refreshes today. `_refresh_all` invokes each registered adapter's `refresh(dayobs)` sequentially and continues after an individual adapter failure. Instrument/dayobs adapters refresh both `lsstcam` and `latiss`.

The refresh registry order is:

1. ConsDB exposures
2. Visit overhead
3. ConsDB visits
4. Expected exposures
5. Exposure log
6. Jira observations
7. Narrative log
8. Night report
9. Rubin Nights context
10. Rubin Nights dome
11. Rubin Nights observatory status

The ordering is significant: visit overhead consumes the exposure rows cached by the first adapter. Among these adapters, direct EFD access occurs in Rubin Nights context, dome, and observatory status on refresh; visit overhead reaches EFD only when exposures are available. ConsDB exposures may conditionally include the transformed-EFD SQL join described above. The other registered adapters use ConsDB, REST, or other non-EFD sources.

### Producer

There is no `Producer` symbol or implementation in this workspace. The verified refresh path is the `run_refresh_worker` entry point and `RefreshWorker`; no separate Producer call chain is documented here.

## Source Map

- Local entry points and registration: [`rubin_nights_context.py`](python/lsst/ts/logging_and_reporting/adapters/rubin_nights_context.py), [`rubin_nights_dome.py`](python/lsst/ts/logging_and_reporting/adapters/rubin_nights_dome.py), [`rubin_nights_obs_status.py`](python/lsst/ts/logging_and_reporting/adapters/rubin_nights_obs_status.py), [`visit_overhead.py`](python/lsst/ts/logging_and_reporting/adapters/visit_overhead.py), [`consdb_exposures.py`](python/lsst/ts/logging_and_reporting/adapters/consdb_exposures.py).
- Schedule: [`run_refresh_worker.py`](python/lsst/ts/logging_and_reporting/run_refresh_worker.py), [`refresh_worker.py`](python/lsst/ts/logging_and_reporting/refresh_worker.py), [`adapters/__init__.py`](python/lsst/ts/logging_and_reporting/adapters/__init__.py), [`cache_ttl.py`](python/lsst/ts/logging_and_reporting/cache_ttl.py).
- Nested installed implementation: `rubin_nights/scriptqueue.py`, `rubin_nights/observatory_status.py`, `rubin_nights/rubin_scheduler_addons.py`, and `rubin_nights/influx_query.py` under the active environment's `site-packages`.
- Existing worker cadence and ordering tests: [`test_refresh_worker.py`](tests/test_refresh_worker.py).
