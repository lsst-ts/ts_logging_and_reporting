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

"""Service for the /expected-exposures endpoint."""

import functools
import logging

from fastapi import HTTPException
from rubin_sim.sim_archive import NoMatchingSimulationsFoundError

from lsst.ts.logging_and_reporting.adapters.expected_exposures import get_expected_exposures_adapter
from lsst.ts.logging_and_reporting.web_app.service import Service

logger = logging.getLogger(__name__)


class ExpectedExposuresService(Service):
    """Sums the expected (simulated) visit count over a dayobs range."""

    def handle_request(self, day_obs_start: int, day_obs_end: int) -> dict:
        """Return the summed expected-exposure count for the range.

        Parameters
        ----------
        day_obs_start, day_obs_end : `int`
            Inclusive bounds of the dayobs range, in YYYYMMDD form.
        """
        try:
            per_day = self.adapters["expected_exposures"].fetch(day_obs_start, day_obs_end)
        except NoMatchingSimulationsFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        response = self.collate_response(per_day)
        logger.debug(
            f"Summed {response['sum_exposures']} expected exposures for "
            f"dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end}"
        )
        return response

    def collate_response(self, data: dict[int, int]) -> dict:
        return {"sum_exposures": sum(data.values())}


@functools.cache
def get_expected_exposures_service() -> ExpectedExposuresService:
    return ExpectedExposuresService(adapters={"expected_exposures": get_expected_exposures_adapter()})
