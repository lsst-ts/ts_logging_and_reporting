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

"""Service for the /context-feed endpoint."""

import functools
import logging

from lsst.ts.logging_and_reporting.adapters.rubin_nights_context import (
    CONTEXT_FEED_COLS,
    RubinNightsContextAdapter,
    get_rubin_nights_context_adapter,
)
from lsst.ts.logging_and_reporting.services.base_service import Service

logger = logging.getLogger(__name__)


class ContextFeedService(Service):
    """Collates consolidated context-feed messages for a dayobs range."""

    def __init__(self, context_adapter: RubinNightsContextAdapter | None = None) -> None:
        self.context_adapter = (
            context_adapter if context_adapter is not None else get_rubin_nights_context_adapter()
        )

    def handle(self, day_obs_start: int, day_obs_end: int) -> dict:
        # dayObsEnd is _inclusive_ here
        records = self.collate_response(self.context_adapter.fetch(day_obs_start, day_obs_end))
        logger.debug(
            f"Collated {len(records)} context feed records for "
            f"dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end}"
        )
        return {"data": records, "cols": CONTEXT_FEED_COLS}

    def collate_response(self, data: dict[int, list[dict]]) -> list[dict]:
        """Flatten per-dayobs buckets into one time-ordered stream."""

        records = [record for dayobs in sorted(data) for record in data[dayobs]]
        if not records:
            return records

        # Recompute task-change rows timestampProcessEnd
        task_changes = [i for i, r in enumerate(records) if r.get("finalStatus") == "Task Change"]
        for position, index in enumerate(task_changes):
            next_position = position + 1
            if next_position < len(task_changes):
                records[index]["timestampProcessEnd"] = records[task_changes[next_position]][
                    "timestampProcessStart"
                ]
            else:
                records[index]["timestampProcessEnd"] = records[-1]["time"]
        return records


@functools.cache
def get_context_feed_service() -> ContextFeedService:
    return ContextFeedService()
