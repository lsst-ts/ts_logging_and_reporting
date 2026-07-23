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

"""Service for the /obs-status endpoint."""

import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.almanac import get_almanac_adapter
from lsst.ts.logging_and_reporting.adapters.rubin_nights_obs_status import (
    get_rubin_nights_obs_status_adapter,
)
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days
from lsst.ts.logging_and_reporting.utils.obs_status import (
    COUNT_STATE_METRIC_MAP,
    build_ms_dayobs_intervals,
    build_ms_night_intervals,
    get_availability,
    get_obs_status_intervals,
    sum_interval_overlap,
)

logger = logging.getLogger(__name__)


class ObsStatusService(Service):
    """Collates observatory-status events, intervals, and metrics.

    dayObsEnd is **inclusive** here (the metric/availability windows and
    the event query all cover the final dayobs), unlike the exclusive-end
    convention used by most endpoints.
    """

    def handle_request(
        self,
        day_obs_start: int,
        day_obs_end: int,
        include_entries: bool = True,
        include_intervals: bool = False,
        night_only_metrics: bool = True,
        requested_metrics: list[str] | None = None,
    ) -> dict:
        entries = self.collate_response(
            self.adapters["obs_status"].fetch(add_or_subtract_dayobs_days(day_obs_start, -1), day_obs_end)
        )

        response: dict[str, Any] = {}
        if include_entries:
            response["entries"] = entries

        if include_intervals or requested_metrics:
            intervals = get_obs_status_intervals(entries)
        if include_intervals:
            response["intervals"] = intervals

        if requested_metrics:
            if night_only_metrics:
                windows = self._night_windows(day_obs_start, day_obs_end)
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

    def _night_windows(self, day_obs_start: int, day_obs_end: int) -> list[dict]:
        """Night twilight windows for each observing night in the range.

        The almanac record for observing night N is keyed under N + 1 (its
        morning boundary), so the night windows for ``[start, end]`` come
        from almanac dayobs ``[start + 1, end + 1]``.
        """
        almanac = self.adapters["almanac"].fetch(
            add_or_subtract_dayobs_days(day_obs_start, 1),
            add_or_subtract_dayobs_days(day_obs_end, 1),
        )
        records = [almanac[dayobs] for dayobs in sorted(almanac)]
        return build_ms_night_intervals(records)


@functools.cache
def get_obs_status_service() -> ObsStatusService:
    return ObsStatusService(
        adapters={
            "obs_status": get_rubin_nights_obs_status_adapter(),
            "almanac": get_almanac_adapter(),
        }
    )
