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

"""Cached adapter for the Night Report API."""

import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.adapters.base_clients import RestClient
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days

logger = logging.getLogger(__name__)


class NightReportCachedAdapter(RestClient, DayobsCachedAdapter):
    """Fetches and caches Night Report records per dayobs."""

    name = "nightreport"

    def __init__(self, redis: Any, server_url: str | None = None, page_limit: int = 100):
        super().__init__(redis, server_url=server_url)
        self._page_limit = page_limit

    def _fetch_run(self, run_start: int, run_end: int) -> dict[int, list[dict]]:
        reports = self._get_json_paged(
            f"{self.server}/nightreport/reports",
            params={
                "is_human": "either",
                "is_valid": "true",
                "order_by": "-day_obs",
                "min_day_obs": run_start,
                # The upstream max_day_obs parameter is exclusive.
                "max_day_obs": add_or_subtract_dayobs_days(run_end, 1),
            },
            page_limit=self._page_limit,
        )
        return self._partition_by_field(reports)


@functools.cache
def get_nightreport_adapter() -> NightReportCachedAdapter:
    return NightReportCachedAdapter(get_redis_client())
