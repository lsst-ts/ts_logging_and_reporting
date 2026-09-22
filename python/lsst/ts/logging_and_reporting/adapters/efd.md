# EFD adapter bits

what do we want instead of a rubin nights adapter for observatory status?

why are we handling add or subtract dayobs in both services/obs_status.py::ObsStatusService.handle() AND in adapters/rubin_nights_obs_status.py::RubinNightsObsStatusAdapter._fetch_run()

## current flow through the adapter layer

ObsStatusService calls ObsStatusAdapter (RubinNightsObsStatusAdapter(RubinNightsMixin and DayobsCachedAdapter)) fetch()

DayobsCachedAdapter::fetch() ->
    CachedAdapter::_fetch_cached() ->
    DayobsCachedAdapter::_fetch_from_source() ->
    CachedAdapter::_collate_runs(keys/dayobslist, self._fetch_run) ->
    CachedAdapter::_fetch_runs_in_parallel(.., fetch_run = self._fetch_run) ->
    ThreadPoolExecutor.submit(..., self._fetch_one_run, fetch_run = self._fetch_run) ->
    CachedAdapter::_fetch_one_run() -> finally calls whatever's been passed as fetch_run
    DayobsCachedAdapter::_fetch_run -> implemented in RubinNightsObsStatusAdapter::_fetch_run

- _fetch_cached() is the main cache loop that takes the keys as dayobs from the dayobs adapter
  - calls _fetch_from_source(to_fetch is a list of dayobs in this case)
  - _fetch_from_source calls _collate_runs which fetches runs in a loop or in parallel if parallel allowed
    - collate takes a dayobs list, so does it not work well with other keys?
    - what is the use case that we have multiple dayobs but they are not contiguous_runs? - when one or some dayobs in the middle of a user requested range are already cached
    - what is the case that an upstream returns data outside of the requested dayobs range?
    - why are we using a break in threadpool executor loop?

RubinNightsObsStatusAdapter only defines _fetch_run()

## oook but the actual logic of _fetch_run is what's adapting to rubin_nights

Start and end times are manipulated astropy.Time versions of get_utc_datetime_from_dayobs_str +- a day

then we send those properly formatted dayobs values to self._efd_client

## flow through RubinNightsClientsMixin

starting with self._efd_client.select_time_series()
def _efd_client -> self._clients["efd"]
_clients -> rubin_nights.connections.get_clients

- handles auth via get access token(tokenfile)
- figures out the site we are at based on EXTERNAL_INSTANCE_URL
  - why is API_ENDPOINTS different from EXTERNAL_INSTANCE_URL, and why do we sort out the site name different from this value?
- sets up client to every database
- efd_client is set to RubinNightsInfluxClient
- some env var setting for usdf and rubin sim data dir and S3 locations
- set the endpoints dictionary & return it with all endpoints

_efd_client is an RubinNights::InfluxQueryClient
__init__
    adds "_efd" to db_name and keeps routing to "usdf" prod
    handles some creds stuff
    creates an httpx client or async client with the url and auth to the efd
we call select_time_series with
    OBS_STATUS_TOPIC = "lsst.sal.Scheduler.logevent_observatoryStatus"
    OBS_STATUS_FIELDS = ["status", "note", "statusLabels"]
    and our extramodified dayobs range
verify (TODO) that select_time_series matches my efd_client's select_time_series
    calls _time_series_query
        checks that topic name exists
        applies old index if needed
        build_influxdb_query (TODO) verify that this is the same as efd_client
            writes the literal influxdb query string


### qualms

- since we want to be deployed on the summit we need to use the summit software team's efd_client
- there's not much error handling in the setting up of the clients
  - it's inefficient/exposes us to extraneous errors by creating a client to everything possible
- I don't think it's appropriate that we have a duplicate of the creds, auth, and influx client access in rubin_nights package
  - is that still what Angelo is suggesting to do?

# Plan anew

use the efd client and essentially mimic RubinNightsClientsMixin
- wouldn't need the auth token? because of repertoire
then in rubin_nights_obs_status.py RubinNightsObsStatusAdapter create a new adapter that is essentially identical but called EFDObsStatusAdapter(NewEFDClientMixin, DayobsCachedAdapter)
- only possible change is the add or subtract or not dayobs end stuff to clean it up, the rest of the logic is not based on rubin_nights
changes in obs_status.py ObsStatusService are minimal, just change the name of the RubinNightsObsStatusAdapter

## secondary pass through

where else do we use RubinNightsClientsMixin?
Can we substitute the efd client for our efd client?
Are we using any other rubin nights clients?
revisit services/obs_status.py get_obs_status_intervals