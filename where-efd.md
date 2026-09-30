# everywhere we use efd from nightly digest

Every call is synchronous
The query templates below show the InfluxQL built by the installed `rubin_nights.InfluxQueryClient`; `{t_start.utc.isot}`, `{t_end.utc.isot}`, and other brace-delimited values are runtime substitutions. A `select_time_series` index clause is included only when the index is truthy.

## rubin nights context adapter

### adapters/rubin_nights_context.py

- get_consolidated_messages(.. clients)
  - uses efd, narrativelog, exposurelog, obsenv

### rubin_nights/scriptqueue.py

- get_script_status (per scriptqueue)

    ```python
    topic = "lsst.sal.ScriptQueue.logevent_summaryState"
    fields = ["salIndex", "summaryState"]
    dd: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
    ```

    InfluxQL:

    ```sql
    SELECT salIndex, summaryState FROM "lsst.sal.ScriptQueue.logevent_summaryState" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

    When the helper checks individual queues after finding ENABLED events, it also sends:

    ```sql
    SELECT salIndex, summaryState FROM "lsst.sal.ScriptQueue.logevent_summaryState" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z' AND salIndex = {queue}
    ```

    - get_script_stream (multiple times)

        ```python
        topic = "lsst.sal.Script.logevent_description"
        fields = ["classname", "description", "salIndex"]
        scriptdescription: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
        ```

        InfluxQL:

        ```sql
        SELECT classname, description, salIndex FROM "lsst.sal.Script.logevent_description" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
        ```

        ```python
        topic = "lsst.sal.Script.command_configure"
        fields = ["blockId", "config", " executionId", "salIndex"]
        scriptconfig: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
        ```

        InfluxQL (including the leading space in the `executionId` field name):

        ```sql
        SELECT blockId, config,  executionId, salIndex FROM "lsst.sal.Script.command_configure" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
        ```

    - get_script_state (multiple times)

        ```python
        topic = "lsst.sal.ScriptQueue.logevent_script"
        fields = [
                "blockId",
                "path",
                "processState",
                "scriptState",
                "salIndex",
                "scriptSalIndex",
                "timestampProcessStart",
                "timestampConfigureStart",
                "timestampConfigureEnd",
                "timestampRunStart",
                "timestampProcessEnd",
        ]
        scripts: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end, index=queue_index)
        ```

        InfluxQL (the `salIndex` clause is omitted when `queue_index` is `None`):

        ```sql
        SELECT blockId, path, processState, scriptState, salIndex, scriptSalIndex, timestampProcessStart, timestampConfigureStart, timestampConfigureEnd, timestampRunStart, timestampProcessEnd FROM "lsst.sal.ScriptQueue.logevent_script" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z' AND salIndex = {queue_index}
        ```

---

- get_scriptqueue_tracebacks(efd)

    ```python
    query = 'select message, traceback, salIndex from "lsst.sal.Script.logevent_logMessage"'
    query += f"where time >= '{t_start.isot}Z' and time <= '{t_end.isot}Z' and traceback != ''"
    ```

    Exact raw query string passed to `query` (note the missing space before `where` in the installed helper):

    ```sql
    select message, traceback, salIndex from "lsst.sal.Script.logevent_logMessage"where time >= '{t_start.isot}Z' and time <= '{t_end.isot}Z' and traceback != ''
    ```

    ```python
    traceback_messages: pd.DataFrame = efd_client.query(query)
    ```

- get_scheduler_configs(efd, obsenv)

    ```python
    # The configurationApplied event should happen with every scheduler Enable.
    topic = "lsst.sal.Scheduler.logevent_configurationApplied"
    fields = ["SchedulerId", "configurations", "salIndex", "schemaVersion", "url", "version"]
    conf_start: pd.DataFrame = efd_client.select_top_n(
        topic, fields, num=1, time_cut=t_start, index=queue
    )
    ```

    InfluxQL:

    ```sql
    SELECT SchedulerId, configurations, salIndex, schemaVersion, url, version FROM "lsst.sal.Scheduler.logevent_configurationApplied" WHERE time <= '{t_start.utc.isot}Z' AND salIndex = {queue} GROUP BY * ORDER BY DESC LIMIT 1
    ```

    ```python
    conf: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end, index=queue)
    ```

    InfluxQL:

    ```sql
    SELECT SchedulerId, configurations, salIndex, schemaVersion, url, version FROM "lsst.sal.Scheduler.logevent_configurationApplied" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z' AND salIndex = {queue}
    ```

    ```python
    # Scheduler dependency information is updated independently of obsenv.
    topic = "lsst.sal.Scheduler.logevent_dependenciesVersions"
    fields = [
        "cloudModel",
        "downtimeModel",
        "seeingModel",
        "skybrightnessModel",
        "observatoryLocation",
        "observatoryModel",
        "scheduler",
        "salIndex",
        "version",
    ]
    deps_start: pd.DataFrame = efd_client.select_top_n(
        topic, fields, num=1, time_cut=Time(conf.index[0]), index=queue
    )
    ```

    InfluxQL:

    ```sql
    SELECT cloudModel, downtimeModel, seeingModel, skybrightnessModel, observatoryLocation, observatoryModel, scheduler, salIndex, version FROM "lsst.sal.Scheduler.logevent_dependenciesVersions" WHERE time <= '{Time(conf.index[0]).utc.isot}Z' AND salIndex = {queue} GROUP BY * ORDER BY DESC LIMIT 1
    ```

    ```python
    deps: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end, index=queue)
    ```

    InfluxQL:

    ```sql
    SELECT cloudModel, downtimeModel, seeingModel, skybrightnessModel, observatoryLocation, observatoryModel, scheduler, salIndex, version FROM "lsst.sal.Scheduler.logevent_dependenciesVersions" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z' AND salIndex = {queue}
    ```

- get_narrative_and_errors(efd, narrativelog)

    ```python
    messages = narrative_log_client.query_log(t_start, t_end)
    topic = "lsst.sal.Scheduler.logevent_observatoryStatus"
    fields = ["status", "note", "statusLabels"]
    obs_status_messages: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
    ```

    InfluxQL:

    ```sql
    SELECT status, note, statusLabels FROM "lsst.sal.Scheduler.logevent_observatoryStatus" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

  - get_error_codes

    ```python
    topics = efd_client.get_topics()
    ```

    `get_topics()` discovers measurements with this query on a cache miss:

    ```sql
    show measurements
    ```

    ```python
    df: pd.DataFrame = efd_client.select_time_series(topic, ["errorCode", "errorReport"], t_start, t_end)
    ```

    InfluxQL (once for each discovered topic containing `errorCode`):

    ```sql
    SELECT errorCode, errorReport FROM "{topic}" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

  - get_all_tracebacks

    ```python
    topics = efd_client.get_topics()
    ```

    `get_topics()` uses the same cached measurement list; on a cache miss, it sends:

    ```sql
    show measurements
    ```

    ```python
    query = f'select * from "{topic}"'
    query += f"where time >= '{t_start.isot}Z' and time <= '{t_end.isot}Z' and traceback != ''"
    ```

    Exact raw query string passed to `query` (note the missing space before `where` in the installed helper):

    ```sql
    select * from "{topic}"where time >= '{t_start.isot}Z' and time <= '{t_end.isot}Z' and traceback != ''
    ```

    ```python
    traceback_messages: pd.DataFrame = efd_client.query(query)
    ```

- get_exposure_info(efd, exposurelog)

    ```python
    # Find exposure information - Simonyi Tel
    topic = "lsst.sal.MTCamera.logevent_endOfImageTelemetry"
    fields = [
        "imageName",
        "imageIndex",
        "exposureTime",
        "darkTime",
        "measuredShutterOpenTime",
        "additionalValues",
        "timestampAcquisitionStart",
        "timestampDateEnd",
        "timestampDateObs",
    ]
    image_acquisition_mt: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
    ```

    InfluxQL:

    ```sql
    SELECT imageName, imageIndex, exposureTime, darkTime, measuredShutterOpenTime, additionalValues, timestampAcquisitionStart, timestampDateEnd, timestampDateObs FROM "lsst.sal.MTCamera.logevent_endOfImageTelemetry" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

    ```python
    topic = "lsst.sal.CCCamera.logevent_endOfImageTelemetry"
    fields = [
        "imageName",
        "imageIndex",
        "exposureTime",
        "darkTime",
        "measuredShutterOpenTime",
        "additionalValues",
        "timestampAcquisitionStart",
        "timestampDateEnd",
        "timestampDateObs",
    ]
    image_acquisition_cc: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
    ```

    InfluxQL:

    ```sql
    SELECT imageName, imageIndex, exposureTime, darkTime, measuredShutterOpenTime, additionalValues, timestampAcquisitionStart, timestampDateEnd, timestampDateObs FROM "lsst.sal.CCCamera.logevent_endOfImageTelemetry" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

    ```python
    # Find exposure information - Aux Tel
    topic = "lsst.sal.ATCamera.logevent_endOfImageTelemetry"
    fields = [
        "imageName",
        "imageIndex",
        "exposureTime",
        "darkTime",
        "measuredShutterOpenTime",
        "additionalValues",
        "timestampAcquisitionStart",
        "timestampDateEnd",
        "timestampDateObs",
    ]
    image_acquisition_at: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
    ```

    InfluxQL:

    ```sql
    SELECT imageName, imageIndex, exposureTime, darkTime, measuredShutterOpenTime, additionalValues, timestampAcquisitionStart, timestampDateEnd, timestampDateObs FROM "lsst.sal.ATCamera.logevent_endOfImageTelemetry" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

    ```python
    exp_logs = exposure_log_client.query_log(t_start, t_end)
    ```

- block_names =

    ```python
    topic = "lsst.sal.Scheduler.command_addBlock"
    block_names = endpoints["efd"].select_time_series(topic, ["id", "salIndex"], t_start, t_end, index=None)
    ```

    InfluxQL:

    ```sql
    SELECT id, salIndex FROM "lsst.sal.Scheduler.command_addBlock" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
    ```

## rubin nights dome adapter

*adapters/rubin_nights_dome.py*

calls rubin_nights/observatory_statpus.py::get_dome_open_close(efd client)

```python
open_query = (
    "SELECT positionActual0, positionActual1, "
    "positionCommanded0, positionCommanded1 FROM "
    '"lsst.sal.MTDome.apertureShutter" WHERE '
    f"time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z' "
    "AND (abs(positionCommanded0) = 100 and abs(positionCommanded1) = 100) "
    "AND (abs(positionActual0) >= 25 and abs(positionActual0) <= 85) "
    "and (abs(positionActual1) >= 25 and abs(positionActual1) <= 85)"
)
dome_shutter_open: pd.DataFrame = efd_client.query(open_query)
```

The dome closes a bit faster than it opens, so the close query uses a wider range of `positionActual` values:

```python
close_query = (
    "SELECT positionActual0, positionActual1, "
    "positionCommanded0, positionCommanded1 FROM "
    '"lsst.sal.MTDome.apertureShutter" WHERE '
    f"time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z' "
    "AND (abs(positionCommanded0) = 0 and abs(positionCommanded1) = 0) "
    "AND (abs(positionActual0) >= 25 and abs(positionActual0) <= 85) "
    "and (abs(positionActual1) >= 25 and abs(positionActual1) <= 85)"
)
dome_shutter_close: pd.DataFrame = efd_client.query(close_query)
```

## rubin nights observatory status adapter

*adapters/rubin_nights_obs_status.py*

uses self._efd_client.select_time_series

```python
OBS_STATUS_TOPIC = "lsst.sal.Scheduler.logevent_observatoryStatus"
OBS_STATUS_FIELDS = ["status", "note", "statusLabels"]
frame = self._efd_client.select_time_series(OBS_STATUS_TOPIC, OBS_STATUS_FIELDS, t_start, t_end)
```

InfluxQL:

```sql
SELECT status, note, statusLabels FROM "lsst.sal.Scheduler.logevent_observatoryStatus" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
```

does not actually use rubin_nights?

- if we provide our own efd client, this is where we are targeting to do that

## visit overhead adapter

*adapters/visit_overhead.py*

The adapter loads exposures, augments the visit data, then calls `add_model_slew_times`, which uses `get_tma_limits` to retrieve mount settings.

Elevation settings:

```python
topic = "lsst.sal.MTMount.logevent_elevationControllerSettings"
el_mapping = {
    "minL1Limit": "altitude_minpos",
    "maxL1Limit": "altitude_maxpos",
    "maxMoveVelocity": "altitude_maxspeed",
    "maxMoveAcceleration": "altitude_accel",
    "maxMoveJerk": "altitude_jerk",
}
fields = list(el_mapping.keys())
elevation_start: pd.DataFrame = efd_client.select_top_n(topic, fields, num=1, time_cut=t_start)
```

InfluxQL:

```sql
SELECT minL1Limit, maxL1Limit, maxMoveVelocity, maxMoveAcceleration, maxMoveJerk FROM "lsst.sal.MTMount.logevent_elevationControllerSettings" WHERE time <= '{t_start.utc.isot}Z' GROUP BY * ORDER BY DESC LIMIT 1
```

```python
elevation: pd.DataFrame = efd_client.select_time_series(topic, fields, t_start, t_end)
```

InfluxQL:

```sql
SELECT minL1Limit, maxL1Limit, maxMoveVelocity, maxMoveAcceleration, maxMoveJerk FROM "lsst.sal.MTMount.logevent_elevationControllerSettings" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
```

Azimuth settings:

```python
topic = "lsst.sal.MTMount.logevent_azimuthControllerSettings"
az_mapping = {
    "minL1Limit": "azimuth_minpos",
    "maxL1Limit": "azimuth_maxpos",
    "maxMoveVelocity": "azimuth_maxspeed",
    "maxMoveAcceleration": "azimuth_accel",
    "maxMoveJerk": "azimuth_jerk",
}
fields = list(az_mapping.keys())
azimuth_start = efd_client.select_top_n(topic, fields, num=1, time_cut=t_start)
```

InfluxQL:

```sql
SELECT minL1Limit, maxL1Limit, maxMoveVelocity, maxMoveAcceleration, maxMoveJerk FROM "lsst.sal.MTMount.logevent_azimuthControllerSettings" WHERE time <= '{t_start.utc.isot}Z' GROUP BY * ORDER BY DESC LIMIT 1
```

```python
azimuth = efd_client.select_time_series(topic, fields, t_start, t_end)
```

InfluxQL:

```sql
SELECT minL1Limit, maxL1Limit, maxMoveVelocity, maxMoveAcceleration, maxMoveJerk FROM "lsst.sal.MTMount.logevent_azimuthControllerSettings" WHERE time >= '{t_start.utc.isot}Z' AND time <= '{t_end.utc.isot}Z'
```

## What do the efd calls look like

### select_top_n

selects `num` most recent rows at or before `time_cut`; `rubin_nights` calls shown above request `num=1`.
The client appends `GROUP BY * ORDER BY DESC LIMIT num`, grouping by all tags rather than only `salIndex`.

### select_time_series

checks that the topic exists, then selects within inclusive time bounds. A truthy `index` adds `AND salIndex = index`; `None` (and `0`) leaves the query unfiltered by SAL index.

### query

sends raw query, response with dataframe

### get_topics()

query = 'show measurements'

## what exactly is called every 5 minutes

### RefreshWorker

*run_refresh_worker.py*
*refresh_worker.py*
https://github.com/lsst-ts/ts_logging_and_reporting/blob/ce49f23f6bebaafc0a1d3511e0694381eb27612e/review/BACKEND_REFACTOR_PLAN.md#refreshworker
https://github.com/lsst-ts/ts_logging_and_reporting/blob/0cf1ac44f55e93502a63d712df48a84db83f7ba5/doc/service-adapter-infrastructure.md#12-the-refresh-worker
(Update link once refactor is merged)

Every five minutes (configurable) we refresh the current dayobs set of data for every CachedAdapter (also listed above in /adapters/)
Each adapter's `refresh(dayobs)` is called, and this is what eventually calls `fetch_one_run()`

The RefreshWorker will be rolled out with our new backend refactor at all of our internal deployment locations, once the refactor is merged.

### Producer - static file generator

https://github.com/lsst-ts/ts_logging_and_reporting/compare/develop...sebastian/experimental/static-file-generator#diff-7c23445bf2965e14b9a998b69dfaa11ff2cec5f1e925c1ba2263620d3d01ec08
(update link once merged)

Every five minutes (configurable) we generate files to store in GCP buckets where our public front end will access data from.
Public front end will not directly call any backend functionality.
Calls our backend url endpoints rather than our CachedAdapters or any internal logic.

