# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""Cached adapter for observatory-status events."""

import functools
import logging

from astropy.time import Time

from lsst.ts.logging_and_reporting.adapters.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.adapters.mixins import RubinNightsClientsMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    dayobs_at,
    get_utc_datetime_from_dayobs_str,
)
from lsst.ts.logging_and_reporting.utils.serialization import make_json_safe

logger = logging.getLogger(__name__)

OBS_STATUS_TOPIC = "lsst.sal.Scheduler.logevent_observatoryStatus"
OBS_STATUS_FIELDS = ["status", "note", "statusLabels"]


class RubinNightsObsStatusAdapter(RubinNightsClientsMixin, DayobsCachedAdapter):
    """Fetches and caches observatory-status events per dayobs.

    Events are ``lsst.sal.Scheduler.logevent_observatoryStatus`` state
    changes read from the EFD; each is bucketed into the dayobs of its
    timestamp, so entries stay range-independent.
    """

    name = "rubin_nights_obs_status"

    def _fetch_run(self, run_start: int, run_end: int) -> dict[int, list[dict]]:
        t_start = Time(get_utc_datetime_from_dayobs_str(run_start))
        t_end = Time(get_utc_datetime_from_dayobs_str(add_or_subtract_dayobs_days(run_end, 1)))
        frame = self._efd_client.select_time_series(OBS_STATUS_TOPIC, OBS_STATUS_FIELDS, t_start, t_end)
        if frame is None or frame.empty:
            return {}
        frame = frame.reset_index(names="time")
        frame["time_ms"] = frame["time"].dt.as_unit("ms").astype("int64")
        event_dayobs = [dayobs_at(time) for time in frame["time"]]
        records = make_json_safe(frame.to_dict(orient="records"))
        partition = self._partition_by_field(list(zip(event_dayobs, records)), key=lambda pair: pair[0])
        return {dayobs: [pair[1] for pair in pairs] for dayobs, pairs in partition.items()}


@functools.cache
def get_rubin_nights_obs_status_adapter() -> RubinNightsObsStatusAdapter:
    return RubinNightsObsStatusAdapter(get_redis_client())
