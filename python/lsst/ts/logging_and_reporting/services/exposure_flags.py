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

"""Service for the /exposure-flags endpoint."""

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

# Exposure flags worth surfacing; "unknown" (mapped from upstream
# "none") is deliberately excluded.
FLAG_VALUES = {"questionable", "junk"}


class ExposureFlagsService(Service):
    """Collates flagged exposures for /exposure-flags.

    Uses the shared ``ExposurelogCachedAdapter``, which caches Exposure
    Log messages for all instruments per dayobs; instrument filtering
    happens here at collation time.
    """

    def __init__(self, exposurelog_adapter: ExposurelogCachedAdapter | None = None) -> None:
        self.exposurelog_adapter = (
            exposurelog_adapter if exposurelog_adapter is not None else get_exposurelog_adapter()
        )

    def handle(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
        """Return flagged exposures for the range and instrument.

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
        instrument_messages = {
            dayobs: [m for m in messages if m.get("instrument") == instrument]
            for dayobs, messages in per_day.items()
        }
        if not any(instrument_messages.values()):
            logger.debug(
                f"No messages for dayObsStart: {day_obs_start}, "
                f"dayObsEnd: {day_obs_end} and instrument: {instrument}"
            )
        flagged = {
            dayobs: [m for m in messages if m.get("exposure_flag") in FLAG_VALUES]
            for dayobs, messages in instrument_messages.items()
        }
        response = self.collate_response(flagged)
        logger.debug(
            f"Retrieved {len(response['exposure_flags'])} flagged records "
            f"for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end} "
            f"and instrument: {instrument}"
        )
        return response

    def collate_response(self, data: dict[int, Any]) -> dict:
        flagged = [
            {"obs_id": message["obs_id"], "exposure_flag": message["exposure_flag"]}
            for message in flatten_sorted(data, "date_added")
        ]
        return {"exposure_flags": flagged}


@functools.cache
def get_exposure_flags_service() -> ExposureFlagsService:
    return ExposureFlagsService()
