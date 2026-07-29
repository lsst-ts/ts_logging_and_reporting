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

"""Cached adapter for the consolidated context-feed messages."""

import functools
import logging
from collections import defaultdict

from astropy.time import Time
from rubin_nights.scriptqueue import get_consolidated_messages

from lsst.ts.logging_and_reporting.adapters.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.adapters.mixins import RubinNightsClientsMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    dayobs_at,
    get_utc_datetime_from_dayobs_str,
)
from lsst.ts.logging_and_reporting.utils.serialization import (
    make_json_safe,
    stringify_special_floats,
)

logger = logging.getLogger(__name__)

# The display shortlist get_consolidated_messages returns
CONTEXT_FEED_COLS = [
    "time",
    "name",
    "description",
    "config",
    "script_salIndex",
    "category_index",
    "finalStatus",
    "timestampProcessStart",
    "timestampConfigureStart",
    "timestampConfigureEnd",
    "timestampRunStart",
    "timestampProcessEnd",
]


class RubinNightsContextAdapter(RubinNightsClientsMixin, DayobsCachedAdapter):
    """Fetches and caches consolidated context-feed messages per dayobs.

    Note that for the synthesized task-change rows, the
    ``timestampProcessEnd`` column is bounded by the dayobs range passed
    to get_consolidated_messages and should therefore be recalculated by
    consumers that care about it.
    """

    name = "rubin_nights_context"

    def _fetch_run(self, run_start: int, run_end: int) -> dict[int, list[dict]]:
        t_start = Time(get_utc_datetime_from_dayobs_str(run_start))
        t_end = Time(get_utc_datetime_from_dayobs_str(add_or_subtract_dayobs_days(run_end, 1)))
        frame, _ = get_consolidated_messages(t_start, t_end, self._clients, all_tracebacks=True)
        if frame is None or frame.empty:
            return {}
        frame = frame[frame.columns.intersection(CONTEXT_FEED_COLS)]
        # Bucket by raw timestamp before serialization stringifies it.
        event_dayobs = [dayobs_at(time) for time in frame["time"]]
        # stringify keeps NaN/inf as the tokens the frontend expects;
        # make_json_safe then renders timestamps and numpy scalars.
        records = make_json_safe(frame.map(stringify_special_floats).to_dict(orient="records"))
        partition: dict[int, list[dict]] = defaultdict(list)
        for dayobs, record in zip(event_dayobs, records):
            partition[dayobs].append(record)
        return partition


@functools.cache
def get_rubin_nights_context_adapter() -> RubinNightsContextAdapter:
    return RubinNightsContextAdapter(get_redis_client())
