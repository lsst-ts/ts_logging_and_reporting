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

"""Service for the /narrative-log endpoint.

The ``NarrativelogCachedAdapter`` caches messages for all telescopes
per dayobs; instrument filtering happens here at collation time.
"""

import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.narrativelog import get_narrativelog_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.collation import flatten_sorted
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days

logger = logging.getLogger(__name__)


class NarrativeLogService(Service):
    """Collates Narrative Log messages for /narrative-log."""

    def handle_request(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
        """Return narrative log messages for the range and instrument.

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
        per_day = self.adapters["narrativelog"].fetch(
            day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
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
            f"Fetched {len(response['narrative_log'])} Narrative Log records "
            f"for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end} "
            f"and instrument: {instrument}"
        )
        return response

    def collate_response(self, data: dict[int, Any]) -> dict:
        records = flatten_sorted(data, "date_begin")
        return {
            "narrative_log": records,
            "time_lost_to_weather": sum(m["time_lost"] for m in records if m["time_lost_type"] == "weather"),
            "time_lost_to_faults": sum(m["time_lost"] for m in records if m["time_lost_type"] == "fault"),
        }


@functools.cache
def get_narrative_log_service() -> NarrativeLogService:
    return NarrativeLogService(adapters={"narrativelog": get_narrativelog_adapter()})
