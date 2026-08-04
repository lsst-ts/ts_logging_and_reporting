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
    "UNKNOWN": 0,
    "DAYTIME": 1 << 0,
    "OPERATIONAL": 1 << 1,
    "FAULT": 1 << 2,
    "WEATHER": 1 << 3,
    "DOWNTIME": 1 << 4,
    "IDLE": 1 << 5,
}
OBS_STATUS_AVAILABLE_DAYOBS = 20260225
MILLISECONDS_IN_AN_HOUR = 3600000


def decode_states(mask: int) -> list[str]:
    """Decode a bitmask into a list of observatory state labels.

    ``mask == 0`` is interpreted as ``UNKNOWN`` to reflect the domain
    meaning.
    """
    if mask == 0:
        return ["UNKNOWN"]
    return [
        name for name, bit in OBSERVATORY_STATES.items() if bit != 0 and (mask & bit)
    ]


def is_unknown(status: int) -> bool:
    """Whether the status is the ``UNKNOWN`` observatory state."""
    return status == OBSERVATORY_STATES["UNKNOWN"]


def contains_daytime(status: int) -> bool:
    """Whether the ``DAYTIME`` bit is set."""
    return bool(status & OBSERVATORY_STATES["DAYTIME"])


def contains_operational(status: int) -> bool:
    """Whether the ``OPERATIONAL`` bit is set."""
    return bool(status & OBSERVATORY_STATES["OPERATIONAL"])


def contains_fault(status: int) -> bool:
    """Whether the ``FAULT`` bit is set."""
    return bool(status & OBSERVATORY_STATES["FAULT"])


def contains_weather(status: int) -> bool:
    """Whether the ``WEATHER`` bit is set."""
    return bool(status & OBSERVATORY_STATES["WEATHER"])


def contains_downtime(status: int) -> bool:
    """Whether the ``DOWNTIME`` bit is set."""
    return bool(status & OBSERVATORY_STATES["DOWNTIME"])


def contains_idle(status: int) -> bool:
    """Whether the ``IDLE`` bit is set."""
    return bool(status & OBSERVATORY_STATES["IDLE"])


def counts_as_fault_loss(status: int) -> bool:
    """Whether a status counts as fault time loss.

    Fault loss is any interval with a ``FAULT`` state, except those also
    carrying a ``DOWNTIME`` state.
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


def get_obs_status_intervals(
    obs_status_entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Convert sequential observatory-status events into interval records.

    Each interval spans the time between consecutive events and records the
    state transition (activated / deactivated states and their labels).
    """
    intervals = []
    for first, second in zip(obs_status_entries, obs_status_entries[1:]):
        first_status = first["status"]
        second_status = second["status"]

        interval_length_ms = second["time_ms"] - first["time_ms"]
        interval_length_hrs = interval_length_ms / MILLISECONDS_IN_AN_HOUR

        changed_mask = first_status ^ second_status
        has_changed = changed_mask != 0

        activated = second_status & ~first_status if has_changed else None
        deactivated = first_status & ~second_status if has_changed else None

        activated_labels = decode_states(activated) if activated else []
        deactivated_labels = decode_states(deactivated) if deactivated else []

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


def build_ms_dayobs_intervals(dayobs_start: int, dayobs_end: int) -> list[dict]:
    """Build millisecond dayobs windows for ``[dayobs_start, dayobs_end]``.

    Each interval runs from 12:00:00 UTC on a dayobs to 11:59:59 UTC on
    the following dayobs, so adjacent dayobs windows do not overlap.
    """
    dayobs_intervals = []
    dayobs = dayobs_start
    while dayobs <= dayobs_end:
        next_dayobs = add_or_subtract_dayobs_days(dayobs, 1)
        dayobs_intervals.append(
            {
                "start_ms": dayobs_to_unix_ms(dayobs),
                # End one second before the next dayobs boundary (12:00 UTC).
                "end_ms": dayobs_to_unix_ms(next_dayobs) - 1000,
            }
        )
        dayobs = next_dayobs
    return dayobs_intervals


def build_ms_night_intervals(almanac_info: list[dict]) -> list[dict]:
    """Build millisecond night windows from almanac twilight boundaries.

    Each record's evening-to-morning 12-degree twilight becomes one night
    interval.
    """
    return [
        {
            "start_ms": almanac_to_unix_ms(day["twilight_evening_12deg"]),
            "end_ms": almanac_to_unix_ms(day["twilight_morning_12deg"]),
        }
        for day in almanac_info
    ]


def sum_interval_overlap(
    event_intervals: list[dict[str, Any]],
    night_intervals: list[dict[str, int]],
    should_count: Callable[[int], bool],
) -> float:
    """Total overlap (hours) between counted event intervals and windows.

    Only event intervals whose ``start_state`` satisfies ``should_count``
    contribute. Both interval lists must be time-ordered.
    """
    total_ms = 0
    night_i = 0
    for interval in event_intervals:
        if not should_count(interval["start_state"]):
            continue
        interval_start = interval["start_time_ms"]
        interval_end = interval["end_time_ms"]
        while night_i < len(night_intervals):
            night = night_intervals[night_i]
            night_start = night["start_ms"]
            night_end = night["end_ms"]
            # Interval is before this night -> next interval.
            if interval_end <= night_start:
                break
            # Interval is after this night -> next night.
            if interval_start >= night_end:
                night_i += 1
                continue
            # Overlap -> accumulate.
            total_ms += max(
                0, min(interval_end, night_end) - max(interval_start, night_start)
            )
            # Interval finishes before the night ends -> next interval.
            if interval_end <= night_end:
                break
            night_i += 1
    return total_ms / MILLISECONDS_IN_AN_HOUR


def get_availability(dayobs_start: int, dayobs_end: int) -> dict[str, Any]:
    """Classify obs-status data availability for the dayobs range.

    "full" when the range is entirely on/after ``OBS_STATUS_AVAILABLE_DAYOBS``,
    "none" when entirely before it, "partial" when it straddles the boundary.
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
            obs_status_adapter
            if obs_status_adapter is not None
            else get_rubin_nights_obs_status_adapter()
        )
        self.almanac_adapter = (
            almanac_adapter if almanac_adapter is not None else get_almanac_adapter()
        )

    def handle(
        self,
        day_obs_start: int,
        day_obs_end: int,
        include_entries: bool = True,
        include_intervals: bool = False,
        night_only_metrics: bool = True,
        requested_metrics: list[str] | None = None,
    ) -> dict:
        # The almanac is only needed for night-windowed metrics; fetch it
        # alongside the events when it is, otherwise just the events.
        need_almanac = bool(requested_metrics) and night_only_metrics
        tasks = {
            "obs_status": lambda: self.obs_status_adapter.fetch(
                add_or_subtract_dayobs_days(day_obs_start, -1), day_obs_end
            ),
        }
        # Don't worry about using fetch_concurrently, almanac fetch is basically free
        if need_almanac:
            tasks["almanac"] = lambda: self.almanac_adapter.fetch(
                add_or_subtract_dayobs_days(day_obs_start, 1),
                add_or_subtract_dayobs_days(day_obs_end, 1),
            )
        fetched = self.fetch_concurrently(tasks)

        if isinstance(fetched["obs_status"], Exception):
            raise fetched["obs_status"]
        entries = self.collate_response(fetched["obs_status"])

        response: dict[str, Any] = {}
        if include_entries:
            response["entries"] = entries

        if include_intervals or requested_metrics:
            intervals = get_obs_status_intervals(entries)
        if include_intervals:
            response["intervals"] = intervals

        if requested_metrics:
            if night_only_metrics:
                if isinstance(fetched["almanac"], Exception):
                    raise fetched["almanac"]
                windows = self._night_windows(fetched["almanac"])
            else:
                windows = build_ms_dayobs_intervals(day_obs_start, day_obs_end)
            metrics = {}
            for metric in requested_metrics:
                should_count = COUNT_STATE_METRIC_MAP.get(metric)
                if should_count is None:
                    logger.warning(f"Unknown metric requested: {metric}")
                    continue
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
