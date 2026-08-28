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

"""Service for the /obs-status endpoint.

The observatory status is a bitmask of `OBSERVATORY_STATES`. The module-level
helpers decode it, turn a sequence of status events into intervals, and sum
the overlap of those intervals with night or dayobs windows to produce the
requested metrics; `ObsStatusService` orchestrates them.
"""

import functools
import logging
from collections.abc import Callable
from typing import Any

from lsst.ts.logging_and_reporting.adapters.almanac import (
    AlmanacCachedAdapter,
    get_almanac_adapter,
)
from lsst.ts.logging_and_reporting.adapters.rubin_nights_obs_status import (
    RubinNightsObsStatusAdapter,
    get_rubin_nights_obs_status_adapter,
)
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    almanac_to_unix_ms,
    dayobs_to_unix_ms,
)

logger = logging.getLogger(__name__)

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
    return bool(status & OBSERVATORY_STATES["IDLE"])


def counts_as_fault_loss(status: int) -> bool:
    """Determine whether a status should be counted as fault time loss.

    Fault loss includes any interval with a ``FAULT`` state,
    except for those also with a ``DOWNTIME`` state.

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
        - ``twilight_evening_12deg``
        - ``twilight_morning_12deg``

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
                "start_ms": almanac_to_unix_ms(day["twilight_evening_12deg"]),
                "end_ms": almanac_to_unix_ms(day["twilight_morning_12deg"]),
            }
        )

    return night_intervals


def sum_interval_overlap(
    event_intervals: list[dict[str, Any]],
    night_intervals: list[dict[str, int]],
    should_count: Callable[[int], bool],
) -> float:
    """Compute total overlap between event intervals and night intervals.

    Only event intervals satisfying the provided predicate are counted.
    The overlap is computed in milliseconds and converted to hours.

    Both interval lists must be time-ordered.

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


class ObsStatusService(Service):
    """Collates observatory-status events, intervals, and metrics.

    dayObsEnd is **inclusive** here (the metric/availability windows and
    the event query all cover the final dayobs), unlike the exclusive-end
    convention used by most endpoints.
    """

    def __init__(
        self,
        obs_status_adapter: RubinNightsObsStatusAdapter | None = None,
        almanac_adapter: AlmanacCachedAdapter | None = None,
    ) -> None:
        self.obs_status_adapter = (
            obs_status_adapter if obs_status_adapter is not None else get_rubin_nights_obs_status_adapter()
        )
        self.almanac_adapter = almanac_adapter if almanac_adapter is not None else get_almanac_adapter()

    def handle(
        self,
        day_obs_start: int,
        day_obs_end: int,
        include_entries: bool = True,
        include_intervals: bool = False,
        night_only_metrics: bool = True,
        requested_metrics: list[str] | None = None,
    ) -> dict:
        """Retrieve observatory status data, intervals, and derived metrics.

        Fetches raw observatory status events, optionally converts them
        into intervals, and computes requested metrics by intersecting
        state intervals with night-time or dayobs boundaries. Metadata
        about the availability of data across the requested range is also
        provided.

        Parameters
        ----------
        day_obs_start : `int`
            Start observation day (YYYYMMDD).
        day_obs_end : `int`
            End observation day (YYYYMMDD), inclusive.
        include_entries : `bool`, optional
            If True, include raw event entries in the response.
        include_intervals : `bool`, optional
            If True, include computed interval data in the response.
        night_only_metrics : `bool`, optional
            If False, entries outside night hours contribute to metrics.
        requested_metrics : `list` [`str`] or None, optional
            List of metrics to compute.

        Returns
        -------
        `dict` [`str`, `Any`]
            Dictionary containing any of:
            - entries: raw status events
            - intervals: derived status intervals
            - metrics: computed time-based metrics (hours)
            - availability: data availability

        Raises
        ------
        Exception
            Whatever the adapters raise, unaltered, for `handle_request`
            to map to a status code.
        """
        # The almanac is only needed for night-windowed metrics; fetch it
        # alongside the events when it is, otherwise just the events.
        # The almanac adapter has a non-inclusive end dayobs, so its
        # range is shifted by a day at both ends.
        need_almanac = bool(requested_metrics) and night_only_metrics
        tasks = {
            "obs_status": lambda: self.obs_status_adapter.fetch(
                add_or_subtract_dayobs_days(day_obs_start, -1), day_obs_end
            ),
        }
        # No need for fetch_concurrently, the almanac fetch is basically free
        if need_almanac:
            tasks["almanac"] = lambda: self.almanac_adapter.fetch(
                add_or_subtract_dayobs_days(day_obs_start, 1),
                add_or_subtract_dayobs_days(day_obs_end, 1),
            )
        fetched = self.fetch_concurrently(tasks)

        if isinstance(fetched["obs_status"], Exception):
            raise fetched["obs_status"]

        # Fetch Observatory Status raw event data.
        entries = self.collate_response(fetched["obs_status"])

        response: dict[str, Any] = {}
        if include_entries:
            response["entries"] = entries

        # Only compute intervals if needed.
        if include_intervals or requested_metrics:
            intervals = get_obs_status_intervals(entries)
        if include_intervals:
            response["intervals"] = intervals

        if requested_metrics:
            # Do we want metrics computed for the night only or all day?
            if night_only_metrics:
                if isinstance(fetched["almanac"], Exception):
                    raise fetched["almanac"]
                # Construct night intervals from almanac data.
                windows = self._night_windows(fetched["almanac"])
            else:
                windows = build_ms_dayobs_intervals(day_obs_start, day_obs_end)

            # Create object to store metrics in.
            metrics = {}

            # For each requested metric, collect predicate function,
            # and pass to summation function to compute.
            for metric in requested_metrics:
                should_count = COUNT_STATE_METRIC_MAP.get(metric)
                if should_count is None:
                    logger.warning(f"Unknown metric requested: {metric}")
                    continue

                # Compute metric.
                metrics[metric] = sum_interval_overlap(intervals, windows, should_count)

            response["metrics"] = metrics

        response["availability"] = get_availability(day_obs_start, day_obs_end)
        logger.debug(
            f"Collated {len(entries)} observatory status entries and "
            f"{len(response.get('metrics', {}))} metric(s) for "
            f"dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end}"
        )
        return response

    def collate_response(self, data: dict[int, list[dict]]) -> list[dict]:
        """Flatten per-dayobs event buckets into one time-ordered stream.

        ``data`` spans ``[day_obs_start - 1, day_obs_end]``; the leading
        day supplies the carry-in event (the last event before the range)
        that sets the observatory state the range starts in. The remaining
        days contribute the range's events in dayobs order.
        """
        days = sorted(data)
        if not days:
            return []
        carry_in_day, *range_days = days
        carry_in_bucket = data[carry_in_day]
        entries = [carry_in_bucket[-1]] if carry_in_bucket else []
        for dayobs in range_days:
            entries.extend(data[dayobs])
        return entries

    def _night_windows(self, almanac: dict) -> list[dict]:
        """Night twilight windows for each observing night in the range.

        ``almanac`` is the almanac adapter's per-dayobs records for
        ``[start + 1, end + 1]`` — night N is keyed under its morning
        boundary N + 1 (the offset applied when the fetch is scheduled).
        """
        records = [almanac[dayobs] for dayobs in sorted(almanac)]
        return build_ms_night_intervals(records)


@functools.cache
def get_obs_status_service() -> ObsStatusService:
    return ObsStatusService()
