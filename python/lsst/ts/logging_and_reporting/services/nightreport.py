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

"""Service for the /night-reports endpoint."""

import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.nightreport import get_nightreport_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.collation import flatten_sorted
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days

logger = logging.getLogger(__name__)


class NightReportService(Service):
    """Collates Night Report records for /night-reports."""

    def handle(self, day_obs_start: int, day_obs_end: int) -> dict:
        """Return night reports for the dayobs range.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the API
            contract — the frontend sends end + 1 day).
        """
        per_day = self.adapters["nightreport"].fetch(
            day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
        if not any(per_day.values()):
            logger.debug(f"No reports for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end}")
        response = self.collate_response(per_day)
        logger.debug(
            f"Fetched {len(response['reports'])} Night Report records "
            f"for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end}"
        )
        return response

    def collate_response(self, data: dict[int, Any]) -> dict:
        return {"reports": flatten_sorted(data, "day_obs")}


@functools.cache
def get_night_report_service() -> NightReportService:
    return NightReportService(adapters={"nightreport": get_nightreport_adapter()})
