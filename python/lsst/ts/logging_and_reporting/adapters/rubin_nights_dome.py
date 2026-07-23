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

"""Cached adapter for Simonyi dome open/close times."""

import functools
import logging

from astropy.time import Time
from rubin_nights.observatory_status import get_dome_open_close

from lsst.ts.logging_and_reporting.adapters.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.adapters.mixins import RubinNightsClientsMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    contiguous_runs,
    get_utc_datetime_from_dayobs_str,
)
from lsst.ts.logging_and_reporting.utils.serialization import make_json_safe

logger = logging.getLogger(__name__)


class RubinNightsDomeAdapter(RubinNightsClientsMixin, DayobsCachedAdapter):
    """Fetches and caches dome open/close records per dayobs.

    There is no per-instrument split — the dome serves every instrument,
    so one entry per dayobs is shared. Each cached record is one dome
    open/close period (a night with no opening still yields one row), and
    carries the night-hours / open-hours / twilight columns the
    ``ExposuresService`` needs to compute open and closed hours. The
    underlying query is an EFD (InfluxDB) query wrapped by rubin_nights,
    so the adapter drives ``get_dome_open_close`` directly rather than
    going through a `RestClient`.
    """

    name = "rubin_nights_dome"

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, list[dict]]:
        results: dict[int, list[dict]] = {dayobs: [] for dayobs in dayobs_list}
        for run_start, run_end in contiguous_runs(dayobs_list):
            logger.debug(f"Fetching dome open/close times for dayobs {run_start}..{run_end}")
            # get_dome_open_close's dayobs window is [start, end) at noon
            # UTC, so query to noon after run_end to cover the run.
            t_start = Time(get_utc_datetime_from_dayobs_str(run_start))
            t_end = Time(get_utc_datetime_from_dayobs_str(add_or_subtract_dayobs_days(run_end, 1)))
            frame = get_dome_open_close(t_start, t_end, self._efd_client)
            if frame is None or frame.empty:
                continue
            for record in make_json_safe(frame.to_dict(orient="records")):
                dayobs = int(record["day_obs"])
                if dayobs in results:
                    results[dayobs].append(record)
        return results


@functools.cache
def get_rubin_nights_dome_adapter() -> RubinNightsDomeAdapter:
    return RubinNightsDomeAdapter(get_redis_client())
