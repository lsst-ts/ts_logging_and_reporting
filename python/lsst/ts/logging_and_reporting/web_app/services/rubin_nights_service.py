#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import rubin_nights.augment_visits as rn_aug
import rubin_nights.dayobs_utils as rn_dayobs
import rubin_nights.rubin_scheduler_addons as rn_sch
from astropy.time import Time, TimeDelta
from rubin_nights.connections import get_clients
from rubin_nights.observatory_status import get_dome_open_close
from rubin_nights.scriptqueue import get_consolidated_messages

from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError
from lsst.ts.logging_and_reporting.utils import add_or_subtract_dayobs_days, stringify_special_floats

from .almanac_service import get_almanac

logger = logging.getLogger(__name__)

WAIT_BEFORE_SLEW = 1.45
SETTLE = 2.0
MAX_SCATTER = 120.0
OBSERVATORY_STATES = {
    "UNKNOWN": 0,  # 0
    "DAYTIME": 1 << 0,  # 1
    "OPERATIONAL": 1 << 1,  # 2
    "FAULT": 1 << 2,  # 4
    "WEATHER": 1 << 3,  # 8
    "DOWNTIME": 1 << 4,  # 16
    "IDLE": 1 << 5,  # 32
}
OBS_STATUS_AVAILABLE_DAYOBS = 20260225
MILLISECONDS_IN_AN_HOUR = 3600000


def dayobs_to_noon_utc(dayobs: int) -> Time:
    """Convert a YYYYMMDD dayobs to its noon UTC boundary,
    in the format required for rubin-nights query inputs.
    """
    return Time(
        f"{rn_dayobs.day_obs_int_to_str(dayobs)}T12:00:00",
        format="isot",
        scale="utc",
    )


def get_time_accounting(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    exposures: list,
    auth_token: str = None,
) -> pd.DataFrame:
    """
    Retrieve and process time accounting data for a given instrument
    and range of observation days.
    This function takes a list of exposures and augments them with
    visit information, calculates model slew times, and computes
    valid overheads for each visit.

    Parameters
    ----------
    dayObsStart : int
        The starting dayObs (observation day) for which to retrieve data.
    dayObsEnd : int
        The ending dayObs (observation day) for which to retrieve data.
    instrument : str
        The name of the instrument for which to retrieve time accounting data.
    exposures : list
        A list of exposure dictionaries or objects to be processed.
    auth_token : str, optional
        Authentication token used when connecting to Rubin Observatory
        services.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed visit
        and overhead information.
        Returns an empty DataFrame if no exposures
        are provided or if an error occurs.
    """
    logger.info(
        f"Getting time accounting data for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        if len(exposures) == 0:
            return pd.DataFrame()

        # Get connections to rubin_nights services
        clients = get_clients(auth_token=auth_token)

        exposures_df = pd.DataFrame(exposures)
        visits = rn_aug.augment_visits(exposures_df, "lsstcam", skip_rs_columns=True)

        visits, _ = rn_sch.add_model_slew_times(
            visits, clients["efd"], model_settle=WAIT_BEFORE_SLEW + SETTLE, dome_crawl=False
        )

        valid_overhead = np.min(
            [
                np.where(np.isnan(visits.slew_model.values), 0, visits.slew_model.values) + MAX_SCATTER,
                visits.visit_gap.values,
            ],
            axis=0,
        )
        visits["overhead"] = valid_overhead

        visits = visits.replace([np.inf, -np.inf], np.nan)
        visits = visits.where(pd.notnull(visits), None)

        return visits

    except Exception as e:
        logger.error(
            f"Error in getting time accounting data from rubin_nights through EFD: {e}", exc_info=True
        )
        return pd.DataFrame()


def get_open_close_dome(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = None,
) -> dict:
    """Retrieve the dome open and close times for a specified
    range of observation days and instrument.
    Currently only for Simonyi.

    Parameters
    ----------
    dayObsStart : int
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayObsEnd : int
        The ending observation day (as an integer, e.g., YYYYMMDD).
    instrument : str
        The name of the instrument for which to retrieve dome open/close times.
    auth_token : str, optional
        Authentication token used when connecting to Rubin Observatory
        services.

    Returns
    -------
    dict
        A dictionary containing the dome open and close times.
    """
    logger.info(
        f"Getting open/close dome times from rubin-nights for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        # Get connections to rubin_nights services
        clients = get_clients(auth_token=auth_token)

        day_min = dayobs_to_noon_utc(dayObsStart)
        day_max = dayobs_to_noon_utc(dayObsEnd)
        dome_open = get_dome_open_close(day_min, day_max, clients["efd"])
        return dome_open
    except Exception as e:
        logger.error(f"Error getting open/close dome times from rubin_nights through EFD: {e}", exc_info=True)
        return pd.DataFrame()


def get_context_feed(
    dayobs_start: int,
    dayobs_end: int,
    auth_token: str = None,
) -> list:
    """Retrieve consolidated context feed messages for a given range of
    observation days.

    This function queries the Rubin Observatory services via the
    consolidated ScriptQueue messages, within the specified dayObs
    range. It processes the results into JSON-safe records, ensuring
    special float values (NaN, Infinity) are handled correctly for
    serialization. Only the relevant context feed columns are retained
    from the raw data. Any raw data outside these columns is dropped;
    if raw data is needed in the future, non-serialisable types must
    be handled explicitly (e.g., using jsonable_encoder).

    Parameters
    ----------
    dayobs_start : int
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayobs_end : int
        The ending observation day (as an integer, e.g., YYYYMMDD).
    auth_token : str, optional
        Authentication token used when connecting to external
        services.

    Returns
    -------
    list
        A list containing two elements:
        - records : list of dict
            JSON-safe context feed records, one per row.
        - cols : list of str
            The column names included in the context feed.
        Returns an empty list if an error occurs.
    """
    logger.info(f"Getting context feed data for start: {dayobs_start}, end: {dayobs_end}")
    try:
        # Get connections to rubin_nights services
        endpoints = get_clients(auth_token=auth_token)

        # Convert dayobs_start and dayobs_end to t_start and t_end
        t_start = dayobs_to_noon_utc(dayobs_start)
        t_end = dayobs_to_noon_utc(dayobs_end) + TimeDelta(1, format="jd")

        # Returns pandas dataframe and list
        df, cols = get_consolidated_messages(t_start, t_end, endpoints, all_tracebacks=True)

        # Discard all columns that are not listed in cols
        df_cols_only = df[df.columns.intersection(cols)]

        # Convert special floats (nans and infs) to strings
        # This ensures that JSON serialisation does not fail
        df_safe = df_cols_only.map(stringify_special_floats)
        records = df_safe.to_dict(orient="records")

        return [records, cols]

    except Exception as e:
        logger.error(f"Error retrieving Context Feed data from rubin_nights: {e}", exc_info=True)
        return []


def get_visits(
    dayObsStart: int, dayObsEnd: int, instrument: str, auth_token: str, augment: bool = True
) -> pd.DataFrame:
    """Get visits from rubin-nights for a given dayObs range and instrument.

    Parameters
    ----------
    dayObsStart : int
        The starting dayObs (observation day) for which to retrieve visits.
    dayObsEnd : int
        The ending dayObs (observation day) for which to retrieve visits.
    instrument : str
        The name of the instrument for which to retrieve visits.
    auth_token : str
        Authentication token to connect to consdb.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the visits.
        Returns an empty DataFrame when the query succeeds but has no rows.

    Raises
    ------
    ConsdbQueryError
        Raised when the underlying ConsDB visit query fails.
    """
    logger.info(
        f"Getting visits using rubin-nights for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        clients = get_clients(auth_token=auth_token)

        t_start = dayobs_to_noon_utc(dayObsStart)
        t_end = dayobs_to_noon_utc(dayObsEnd)

        visits = clients["consdb"].get_visits(instrument.lower(), t_start, t_end, augment=augment)

        return visits

    except Exception as e:
        logger.error(f"Error getting visits using rubin-nights: {e}", exc_info=True)
        raise ConsdbQueryError("Failed to query visits from ConsDB") from e


# TODO: Currently, we retrieve from rubin_nights;
# once our new EFD adapter is ready (OSW-2120),
# move to querying through there (OSW-2314).
def get_obs_status_events(
    dayobs_start: int,
    dayobs_end: int,
    auth_token: str = None,
) -> list[dict[str, Any]]:
    """Retrieve observatory status records for a given range of
    observation days.

    The query window is expanded beyond the requested range to
    ensure the initial observatory status can be determined.
    The single event prior to the start of the requested range, if
    it exists, is included in the returned records.

    Parameters
    ----------
    dayobs_start : `int`
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayobs_end : `int`
        The ending observation day (as an integer, e.g., YYYYMMDD).
    auth_token : `str`, optional
        Authentication token used when connecting to external
        services.

    Returns
    -------
    `list` [`dict`]
        A list of observatory status records. The first record may
        precede the requested ``dayobs_start`` and represents the most
        recent observatory status prior to the requested range.

        Returns an empty list if an error occurs.
    """
    logger.info(f"Getting Observatory Status data for start: {dayobs_start}, end: {dayobs_end}")
    try:
        # Get connections to rubin_nights services
        endpoints = get_clients(auth_token=auth_token)

        # Convert dayobs_start and dayobs_end to query start and end times,
        # where we query an extra 12 hours prior to the dayobs_start start
        # time, so we can know the observatory status at the start of the
        # dayobs. At a minimum, this should capture the automated state
        # change at 0 deg sunrise.
        dayobs_start_time = dayobs_to_noon_utc(dayobs_start)
        query_start_time = dayobs_start_time - TimeDelta(0.5, format="jd")
        query_end_time = dayobs_to_noon_utc(dayobs_end) + TimeDelta(1, format="jd")

        # Add ObservatoryStatus topic
        efd_client = endpoints["efd"]
        topic = "lsst.sal.Scheduler.logevent_observatoryStatus"
        fields = ["status", "note", "statusLabels"]
        obs_status_messages: pd.DataFrame = efd_client.select_time_series(
            topic, fields, query_start_time, query_end_time
        )

        # Find the last event before the requested start dayobs.
        dayobs_start_time_dt = pd.Timestamp(dayobs_start_time.to_datetime(), tz="UTC")
        prior_events = obs_status_messages[obs_status_messages.index < dayobs_start_time_dt]
        if not prior_events.empty:
            # Keep the final prior event + everything after it.
            obs_status_messages = obs_status_messages.loc[prior_events.index[-1] :]

        # Set timestamp as a column instead of the index
        # to preserve it in subsequent list transformation
        time_as_col = obs_status_messages.reset_index(names="time")
        # Create a column of Unix ms timestamps for plotting library
        time_as_col["time_ms"] = time_as_col["time"].astype("int64") // 1_000_000
        records = time_as_col.to_dict(orient="records")

        return records

    except Exception as e:
        logger.error(f"Error retrieving Observatory Status data from rubin_nights: {e}", exc_info=True)
        return []


def decode_states(mask: int) -> list[str]:
    """Decode a bitmask into a list of observatory state labels.

    Each bit in the input mask is matched against the OBSERVATORY_STATES
    mapping and converted into its corresponding state name.

    The special case ``mask == 0`` is interpreted as ``UNKNOWN`` to reflect
    the domain meaning.

    Parameters
    ----------
    mask : `int`
        Bitmask representing one or more observatory states.

    Returns
    -------
    `list` [`str`]
        List of state names corresponding to active bits in the mask.
        If ``mask == 0``, returns ``["UNKNOWN"]``.
    """
    if mask == 0:
        return ["UNKNOWN"]

    states = [name for name, bit in OBSERVATORY_STATES.items() if bit != 0 and (mask & bit)]

    return states


# TODO: Lots of (possibly unneeded) metadata is returned
# here, in case it is useful for plotting. During plot
# implementation, it will become apparent what is/isn't
# useful, so remove what is unneeded then (OSW-2118).
def get_obs_status_intervals(
    obs_status_entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Convert sequential observatory status events into interval records.

    Each interval represents the time between consecutive status entries
    and includes state transitions, timing information, and decoded
    state changes.

    Parameters
    ----------
    obs_status_entries : `list` [`str`]
        Ordered list of observatory status event dictionaries. Each entry
        must include at least:
        - ``status``
        - ``time_ms``
        - ``time``
        - ``note``
        - ``statusLabels``

    Returns
    -------
    `list` [`dict`]
        List of interval dictionaries containing timing, state transition
        information, and human-readable decoded state changes.
    """
    intervals = []

    for consecutive_entries in zip(obs_status_entries, obs_status_entries[1:]):
        first = consecutive_entries[0]
        second = consecutive_entries[1]
        first_status = first["status"]
        second_status = second["status"]

        # How long was the interval between entries?
        interval_length_ms = second["time_ms"] - first["time_ms"]
        interval_length_hrs = interval_length_ms / MILLISECONDS_IN_AN_HOUR

        # Was there a status change? What was it?
        changed_mask = first_status ^ second_status
        has_changed = changed_mask != 0

        # How did status change? What states were activated / deactivated?
        activated = second_status & ~first_status if has_changed else None
        deactivated = first_status & ~second_status if has_changed else None

        # Create user-friendly labels for state changes.
        activated_labels = decode_states(activated) if activated else []
        deactivated_labels = decode_states(deactivated) if deactivated else []

        # Check for the UNKNOWN state (bitmask 0) and add labels,
        # if present.
        start_unknown = is_unknown(first_status)
        end_unknown = is_unknown(second_status)

        if end_unknown and not start_unknown:
            activated_labels.append("UNKNOWN")

        if start_unknown and not end_unknown:
            deactivated_labels.append("UNKNOWN")

        intervals.append(
            {
                "start_time_dt": first["time"],
                "end_time_dt": second["time"],
                "start_time_ms": first["time_ms"],
                "end_time_ms": second["time_ms"],
                "interval_length_ms": interval_length_ms,
                "interval_length_hrs": interval_length_hrs,
                "start_state": first_status,
                "end_state": second_status,
                "start_note": first["note"],
                "end_note": second["note"],
                "start_labels": first["statusLabels"],
                "end_labels": second["statusLabels"],
                "changed_mask": changed_mask,
                "has_changed": has_changed,
                "activated": activated,
                "deactivated": deactivated,
                "activated_labels": activated_labels,
                "deactivated_labels": deactivated_labels,
            }
        )

    return intervals


def dayobs_to_unix_ms(dayobs: int, hour: int = 12) -> int:
    """Convert a dayobs int to Unix time in milliseconds (UTC).

    Parameters
    ----------
    dayobs : `int`
        Observation day (YYYYMMDD).
    hour : `int`
        Hour to set, optional.

    Returns
    -------
    `int`
        Unix timestamp in milliseconds.
    """
    date = datetime.strptime(str(dayobs), "%Y%m%d").replace(
        hour=hour,
        tzinfo=timezone.utc,
    )
    return int(date.timestamp() * 1000)


def almanac_to_unix_ms(almanac_time: str) -> int:
    """Convert an almanac timestamp string to Unix time in milliseconds (UTC).

    Parameters
    ----------
    almanac_time : `str`
        Timestamp in format ``YYYY-MM-DD HH:MM:SS``.

    Returns
    -------
    `int`
        Unix timestamp in milliseconds.
    """
    dt = datetime.strptime(almanac_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1000)


def build_ms_dayobs_intervals(
    dayobs_start: int,
    dayobs_end: int,
) -> list[dict]:
    """Construct a set of dayobs intervals from start/end dayobs.

    Each dayobs between the start/end dayobs is converted into a
    millisecond interval covering the corresponding dayobs window.
    Intervals are defined from 12:00:00 UTC on a dayobs to
    11:59:59 UTC on the following dayobs to avoid overlap between
    adjacent dayobs intervals.

    Parameters
    ----------
    dayobs_start : `int`
        Start observation day (YYYYMMDD).
    dayobs_end : `int`
        End observation day (YYYYMMDD).

    Returns
    -------
    `list` [`dict`]
        List of dayobs intervals with:
        - ``start_ms``
        - ``end_ms``
    """
    dayobs_intervals = []

    dayobs = dayobs_start

    while dayobs <= dayobs_end:
        next_dayobs = add_or_subtract_dayobs_days(dayobs, 1)

        start_ms = dayobs_to_unix_ms(dayobs)
        # End one second before the next dayobs boundary (12:00 UTC).
        end_ms = dayobs_to_unix_ms(next_dayobs) - 1000

        dayobs_intervals.append(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
            }
        )

        dayobs = next_dayobs

    return dayobs_intervals


def build_ms_night_intervals(almanac_info: list[dict]) -> list[dict]:
    """Construct night time intervals from almanac twilight boundaries.

    Each input record is converted into a start/end millisecond interval
    representing the observable night window.

    Parameters
    ----------
    almanac_info : `list` [`dict`]
        List of almanac records containing:
        - ``twilight_evening``
        - ``twilight_morning``

    Returns
    -------
    `list` [`dict`]
        List of night intervals with:
        - ``start_ms``
        - ``end_ms``
    """
    night_intervals = []

    for day in almanac_info:
        night_intervals.append(
            {
                "start_ms": almanac_to_unix_ms(day["twilight_evening"]),
                "end_ms": almanac_to_unix_ms(day["twilight_morning"]),
            }
        )

    return night_intervals


# Predicate functions
# -------------------
# The following collection of functions are for mapping
# requested metrics to the combination of observatory
# states required to compute them.


def is_unknown(status: int) -> bool:
    """Check whether the status represents an ``UNKNOWN`` observatory state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
       ``True`` if status is ``UNKNOWN``, otherwise ``False``.
    """
    return status == OBSERVATORY_STATES["UNKNOWN"]


def contains_daytime(status: int) -> bool:
    """Check whether the status includes the ``DAYTIME`` state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if ``DAYTIME`` bit is set.
    """
    return bool(status & OBSERVATORY_STATES["DAYTIME"])


def contains_operational(status: int) -> bool:
    """Check whether the status includes the ``OPERATIONAL`` state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if ``OPERATIONAL`` bit is set.
    """
    return bool(status & OBSERVATORY_STATES["OPERATIONAL"])


def contains_fault(status: int) -> bool:
    """Check whether the status includes the ``FAULT`` state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if ``FAULT`` bit is set.
    """
    return bool(status & OBSERVATORY_STATES["FAULT"])


def contains_weather(status: int) -> bool:
    """Check whether the status includes the ``WEATHER`` state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if ``WEATHER`` bit is set.
    """
    return bool(status & OBSERVATORY_STATES["WEATHER"])


def contains_downtime(status: int) -> bool:
    """Check whether the status includes the ``DOWNTIME`` state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if ``DOWNTIME`` bit is set.
    """
    return bool(status & OBSERVATORY_STATES["DOWNTIME"])


def contains_idle(status: int) -> bool:
    """Check whether the status includes the ``IDLE`` state.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if ``IDLE`` bit is set.
    """
    return bool(status & OBSERVATORY_STATES["DOWNTIME"])


def counts_as_fault_loss(status: int) -> bool:
    """Determine whether a status should be counted as fault time loss.

    Fault loss includes ``FAULT`` and ``FAULT | WEATHER`` states,
    but excludes any interval that includes ``DOWNTIME``.

    Parameters
    ----------
    status : `int`
        Bitmask status value.

    Returns
    -------
    `bool`
        ``True`` if the status contributes to fault time loss.
    """

    has_fault = bool(status & OBSERVATORY_STATES["FAULT"])
    has_downtime = bool(status & OBSERVATORY_STATES["DOWNTIME"])

    return has_fault and not has_downtime


COUNT_STATE_METRIC_MAP = {
    "unknown": is_unknown,
    "daytime": contains_daytime,
    "operational": contains_operational,
    "fault": contains_fault,
    "weather": contains_weather,
    "downtime": contains_downtime,
    "idle": contains_idle,
    "fault_loss": counts_as_fault_loss,
}


def sum_interval_overlap(
    event_intervals: list[dict[str, Any]],
    night_intervals: list[dict[str, int]],
    should_count: Callable[[int], bool],
) -> float:
    """Compute total overlap between event intervals and night intervals.

    Only event intervals satisfying the provided predicate are counted.
    The overlap is computed in milliseconds and converted to hours.

    Parameters
    ----------
    event_intervals : `list` [`dict`]
        List of event intervals with start/end times in milliseconds.
    night_intervals : `list` [`dict`]
        List of night boundaries with start/end times in milliseconds.
    should_count : ``callable``
        Predicate function that takes one argument:

        - ``status``: bitmask status value (`int`).

    Returns
    -------
    `float`
        Total overlapping time in hours.
    """
    # Intialise cumulative total.
    total_ms = 0

    # Initialise night counter.
    night_i = 0

    # Loop through event intervals, considering only
    # intervals with active states of interest, and
    # sum all intervals that overlap with night
    # intervals.
    for interval in event_intervals:
        # Get active states during interval.
        status = interval["start_state"]

        # If no active states are to be counted,
        # skip to the next interval.
        if not should_count(status):
            continue

        # Get event interval boundaries.
        interval_start = interval["start_time_ms"]
        interval_end = interval["end_time_ms"]

        # Compare the current interval against the
        # night intervals, and add any overlap to
        # the cumulative total.
        while night_i < len(night_intervals):
            # Get night boundaries.
            night = night_intervals[night_i]
            night_start = night["start_ms"]
            night_end = night["end_ms"]

            # CASE: Interval is before this night.
            # -> Move to next interval.
            if interval_end <= night_start:
                break

            # CASE: Interval is after this night.
            # -> Move to next night.
            if interval_start >= night_end:
                night_i += 1
                continue

            # CASE: Some of interval is in this night.
            # -> Calculate how much of the event interval
            # overlaps with the night interval.
            overlap_ms = max(0, min(interval_end, night_end) - max(interval_start, night_start))

            # Add overlap to cumulative total.
            total_ms += overlap_ms

            # CASE: Interval finishes before night ends.
            # -> Move to next interval.
            if interval_end <= night_end:
                break

            # Move to next night.
            night_i += 1

    # Convert ms to hrs.
    total_hrs = total_ms / MILLISECONDS_IN_AN_HOUR

    return total_hrs


def get_availability(
    dayobs_start: int,
    dayobs_end: int,
) -> dict[str, Any]:
    """Determine availability of obs_status data over a requested dayobs range.

    Availability is classified relative to ``OBS_STATUS_AVAILABLE_DAYOBS``:
    - "none": range is entirely before availability
    - "full": range is entirely on/after availability
    - "partial": range straddles the availability boundary

    Parameters
    ----------
    dayobs_start : `int`
        Start of requested dayobs range (inclusive).
    dayobs_end : `int`
        End of requested dayobs range (inclusive).

    Returns
    -------
    `dict` [`str`, `Any`]
        Availability metadata including status and availability boundary.
    """

    if dayobs_start >= OBS_STATUS_AVAILABLE_DAYOBS:
        availability_status = "full"
    elif dayobs_end < OBS_STATUS_AVAILABLE_DAYOBS:
        availability_status = "none"
    else:
        availability_status = "partial"

    return {
        "status": availability_status,
        "available_from": OBS_STATUS_AVAILABLE_DAYOBS,
    }


def get_obs_status(
    dayobs_start: int,
    dayobs_end: int,
    include_entries: bool = True,
    include_intervals: bool = False,
    night_only_metrics: bool = True,
    requested_metrics: list[str] | None = None,
    auth_token: str = None,
) -> dict[str, Any]:
    """Retrieve observatory status data, intervals, and derived metrics.

    This function fetches raw observatory status events, optionally
    converts them into intervals, and computes requested metrics by
    intersecting state intervals with night-time or dayobs boundaries.
    Metadata about the availability of data across the requested range
    is also provided.

    Parameters
    ----------
    dayobs_start : `int`
        Start observation day (YYYYMMDD).
    dayobs_end : `int`
        End observation day (YYYYMMDD).
    include_entries : `bool`, optional
        If True, include raw event entries in the response.
    include_intervals : `bool`, optional
        If True, include computed interval data in the response.
    night_only_metrics : `bool`, optional
        If False, entries outside night hours contribute to metrics.
    requested_metrics : `list` [`str`] or None, optional
        List of metrics to compute.
    auth_token : `str`, optional
        Authentication token for external services.

    Returns
    -------
    `dict` [`str`, `any`]
        Dictionary containing any of:
        - entries: raw status events
        - intervals: derived status intervals
        - metrics: computed time-based metrics (hours)
        - availability: data availability

        Returns an empty `dict` on error.
    """
    logger.info(f"Getting Observatory Status data for start: {dayobs_start}, end: {dayobs_end}")

    try:
        response = {}

        # Fetch Observatory Status raw event data.
        obs_status_entries = get_obs_status_events(dayobs_start, dayobs_end, auth_token=auth_token)
        if include_entries:
            response["entries"] = obs_status_entries

        # Only compute intervals if needed.
        need_intervals = include_intervals or bool(requested_metrics)

        # Transform events into intervals.
        if need_intervals:
            obs_status_intervals = get_obs_status_intervals(obs_status_entries)
        if include_intervals:
            response["intervals"] = obs_status_intervals

        if requested_metrics:
            # Do we want metrics computed for the night only or all day?
            if not night_only_metrics:
                dayobs_intervals = build_ms_dayobs_intervals(dayobs_start, dayobs_end)
            else:
                # Get night boundaries from almanac adapter.
                # First, we need to add an extra day because the almanac
                # adapter has a non-inclusive end dayobs.
                almanac_dayobs_end = add_or_subtract_dayobs_days(dayobs_end, 1)
                almanac_info = get_almanac(dayobs_start, almanac_dayobs_end)
                # Construct night intervals from almanac data.
                dayobs_intervals = build_ms_night_intervals(almanac_info)

            # Create object to store metrics in.
            obs_status_metrics = {}

            # For each requested metric, collect predicate function,
            # and pass to summation function to compute.
            for metric in requested_metrics:
                if metric not in COUNT_STATE_METRIC_MAP:
                    logger.warning(f"Unknown metric requested: {metric}")
                    continue

                # Collect predicate function.
                should_count = COUNT_STATE_METRIC_MAP[metric]

                # Compute metric.
                obs_status_metrics[metric] = sum_interval_overlap(
                    obs_status_intervals,
                    dayobs_intervals,
                    should_count,
                )

            response["metrics"] = obs_status_metrics

        response["availability"] = get_availability(dayobs_start, dayobs_end)

        return response

    except Exception as e:
        logger.error(f"Error retrieving Observatory Status data from rubin_nights: {e}", exc_info=True)
        return {}
