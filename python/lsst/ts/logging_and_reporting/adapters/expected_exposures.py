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

"""Cached adapter for the expected-exposures count from the sim archive."""

import functools
import logging

from rubin_sim.sim_archive import fetch_sim_stats_for_night

from lsst.ts.logging_and_reporting.adapters.mixins import MutableDataMixin
from lsst.ts.logging_and_reporting.web_app.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)

# Simulations are only reachable within this many days of the current
# date; a night outside the window has no matching simulation.
MAX_SIMULATION_AGE = 60


class ExpectedExposuresCachedAdapter(MutableDataMixin, DayobsCachedAdapter):
    """Caches the expected (simulated) visit count per night.

    Counts come from the latest nominal pre-night simulation for each
    dayobs (``rubin_sim.sim_archive``). They are mutable — schedules are
    re-simulated — so they get the mutable TTL. A night with no matching
    simulation raises ``NoMatchingSimulationsFoundError``, which the
    service maps to 404.
    """

    name = "expected_exposures"

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, int]:
        return {dayobs: self._nominal_visits(dayobs) for dayobs in dayobs_list}

    def _nominal_visits(self, dayobs: int) -> int:
        logger.debug(f"Fetching expected exposures for dayobs {dayobs}")
        stats = fetch_sim_stats_for_night(day_obs=dayobs, max_simulation_age=MAX_SIMULATION_AGE)
        return stats.get("nominal_visits", 0)


@functools.cache
def get_expected_exposures_adapter() -> ExpectedExposuresCachedAdapter:
    return ExpectedExposuresCachedAdapter(get_redis_client())
