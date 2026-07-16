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

"""Cached adapter for the Exposure Log API."""

import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.http import RestCachedAdapter
from lsst.ts.logging_and_reporting.utils import add_or_subtract_dayobs_days
from lsst.ts.logging_and_reporting.web_app.base_adapter import contiguous_runs
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class ExposurelogCachedAdapter(RestCachedAdapter):
    """Fetches and caches Exposure Log messages per dayobs.

    Messages for **all instruments** are cached together under one
    dayobs key — the services filter by instrument when collating.
    This keeps the cache key purely ``(adapter, dayobs)`` and means
    one upstream fetch (and one ``RefreshWorker`` refresh) covers
    every instrument.
    """

    name = "exposurelog"

    def __init__(self, redis: Any, server_url: str | None = None, limit: int = 2500):
        super().__init__(redis, server_url=server_url)
        self._limit = limit

    def _ttl(self, dayobs: int) -> int:
        """Always the short TTL.

        Exposure log entries can be added or edited for past nights,
        so historical entries must not be cached for long (mirrors
        the always-short list in ``CacheControlMiddleware``).
        """
        return self.SHORT_TTL

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, list[dict]]:
        results: dict[int, list[dict]] = {dayobs: [] for dayobs in dayobs_list}
        # The API takes a min/max dayobs range, so issue one request
        # per contiguous run of missing days.
        for run_start, run_end in contiguous_runs(dayobs_list):
            logger.debug(f"Fetching Exposure Log messages for dayobs {run_start}..{run_end}")
            messages = self._get_json(
                f"{self.server}/exposurelog/messages",
                params={
                    "is_human": "true",
                    "order_by": "-date_added",
                    "limit": self._limit,
                    "min_day_obs": run_start,
                    # The upstream max_day_obs parameter is exclusive.
                    "max_day_obs": add_or_subtract_dayobs_days(run_end, 1),
                },
            )
            for message in messages:
                if message.get("exposure_flag") == "none":
                    message["exposure_flag"] = "unknown"
                dayobs = message.get("day_obs")
                if dayobs in results:
                    results[dayobs].append(message)
        return results


@functools.cache
def get_exposurelog_adapter() -> ExposurelogCachedAdapter:
    return ExposurelogCachedAdapter(get_redis_client())
