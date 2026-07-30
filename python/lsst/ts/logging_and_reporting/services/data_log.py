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

"""Service for the /data-log endpoint."""

import functools
import logging

import pandas as pd

from lsst.ts.logging_and_reporting.adapters.consdb_exposures import (
    ConsdbExposuresAdapter,
    get_consdb_exposures_adapter,
)
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days
from lsst.ts.logging_and_reporting.utils.serialization import make_json_safe, stringify_special_floats

logger = logging.getLogger(__name__)


class DataLogService(Service):
    """Collates the detailed data log for /data-log.

    Returns the full exposure record (exposure ⋈ quicklook ⋈ EFD, as the
    adapter caches it), with special floats (NaN/inf) rendered as
    JSON-safe strings.
    """

    def __init__(self, consdb_adapter: ConsdbExposuresAdapter | None = None) -> None:
        self.consdb_adapter = consdb_adapter if consdb_adapter is not None else get_consdb_exposures_adapter()

    def handle(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
        """Return the detailed data log for the range and instrument.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the API
            contract — the frontend sends end + 1 day).
        instrument : `str`
            Instrument to query (e.g. ``LSSTCam``).
        """
        per_day = self.consdb_adapter.fetch(
            instrument, day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
        response = self.collate_response(per_day)
        logger.debug(
            f"Fetched {len(response['data_log'])} data-log records for dayObsStart: {day_obs_start}, "
            f"dayObsEnd: {day_obs_end} and instrument: {instrument}"
        )
        return response

    def collate_response(self, data: dict[int, list[dict]]) -> dict:
        records = [record for dayobs in sorted(data) for record in data[dayobs]]
        # A DataFrame turns missing numeric values into NaN; stringify
        # renders NaN/inf as strings, and make_json_safe clears the
        # remaining numpy scalar types so the result is JSON-native.
        safe = pd.DataFrame(records).map(stringify_special_floats)
        return {"data_log": make_json_safe(safe.to_dict(orient="records"))}


@functools.cache
def get_data_log_service() -> DataLogService:
    return DataLogService()
