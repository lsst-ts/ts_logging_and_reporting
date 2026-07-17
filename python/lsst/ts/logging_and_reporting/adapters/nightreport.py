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

from lsst.ts.logging_and_reporting.adapters.http import RestCachedAdapter
from lsst.ts.logging_and_reporting.utils import add_or_subtract_dayobs_days
from lsst.ts.logging_and_reporting.web_app.base_adapter import contiguous_runs
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class NightReportCachedAdapter(RestCachedAdapter):
    """Fetches and caches Night Report records per dayobs."""

    name = "nightreport"

    def __init__(self, redis: Any, server_url: str | None = None, page_limit: int = 100):
        super().__init__(redis, server_url=server_url)
        self._page_limit = page_limit

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, list[dict]]:
        results: dict[int, list[dict]] = {dayobs: [] for dayobs in dayobs_list}
        # The API takes a min/max dayobs range, so issue one request
        # per contiguous run of missing days.
        for run_start, run_end in contiguous_runs(dayobs_list):
            logger.debug(f"Fetching Night Report records for dayobs {run_start}..{run_end}")
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
            for report in reports:
                dayobs = report.get("day_obs")
                if dayobs in results:
                    results[dayobs].append(report)
        return results


@functools.cache
def get_nightreport_adapter() -> NightReportCachedAdapter:
    return NightReportCachedAdapter(get_redis_client())
