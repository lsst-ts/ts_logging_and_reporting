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

"""Service for the /exposure-entries endpoint.

Uses the shared ``ExposurelogCachedAdapter``, which caches Exposure
Log messages for all instruments per dayobs; instrument filtering
happens here at collation time.
"""

import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.exposurelog import (
    ExposurelogCachedAdapter,
    get_exposurelog_adapter,
)
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.collation import flatten_sorted
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days

logger = logging.getLogger(__name__)


class ExposureEntriesService(Service):
    """Collates Exposure Log entries for /exposure-entries."""

    def __init__(self, exposurelog_adapter: ExposurelogCachedAdapter | None = None) -> None:
        self.exposurelog_adapter = (
            exposurelog_adapter if exposurelog_adapter is not None else get_exposurelog_adapter()
        )

    def handle(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
        """Return exposure log entries for the range and instrument.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the API
            contract — the frontend sends end + 1 day).
        instrument : `str`
            Instrument to filter by (e.g. ``LSSTCam``).
        """
        per_day = self.exposurelog_adapter.fetch(day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1))
        filtered = {
            dayobs: [m for m in messages if m.get("instrument") == instrument]
            for dayobs, messages in per_day.items()
        }
        if not any(filtered.values()):
            logger.debug(
                f"No messages for dayObsStart: {day_obs_start}, "
                f"dayObsEnd: {day_obs_end} and instrument: {instrument}"
            )
        response = self.collate_response(filtered)
        logger.debug(
            f"Fetched {len(response['exposure_entries'])} Exposure Log records "
            f"for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end} "
            f"and instrument: {instrument}"
        )
        return response

    def collate_response(self, data: dict[int, Any]) -> dict:
        return {"exposure_entries": flatten_sorted(data, "date_added")}


@functools.cache
def get_exposure_entries_service() -> ExposureEntriesService:
    return ExposureEntriesService()
