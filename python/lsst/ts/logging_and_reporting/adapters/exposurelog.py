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
from collections import defaultdict
from typing import Any

from lsst.ts.logging_and_reporting.adapters.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.adapters.base_clients import RestClient
from lsst.ts.logging_and_reporting.adapters.mixins import MutableDataMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days

logger = logging.getLogger(__name__)


class ExposurelogCachedAdapter(MutableDataMixin, RestClient, DayobsCachedAdapter):
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

    def _fetch_run(self, run_start: int, run_end: int) -> dict[int, list[dict]]:
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
        partition: dict[int, list[dict]] = defaultdict(list)
        for message in messages:
            if message.get("exposure_flag") == "none":
                message["exposure_flag"] = "unknown"
            partition[message.get("day_obs")].append(message)
        return partition


@functools.cache
def get_exposurelog_adapter() -> ExposurelogCachedAdapter:
    return ExposurelogCachedAdapter(get_redis_client())
