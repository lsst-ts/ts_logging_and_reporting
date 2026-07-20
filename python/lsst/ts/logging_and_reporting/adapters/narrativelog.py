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

"""Cached adapter for the Narrative Log API."""

import datetime as dt
import functools
import logging
from typing import Any

from lsst.ts.logging_and_reporting.adapters.http import RestCachedAdapter
from lsst.ts.logging_and_reporting.utils import add_or_subtract_dayobs_days, dayobs_at
from lsst.ts.logging_and_reporting.web_app.base_adapter import MutableDataMixin, contiguous_runs
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)

LSSTCAM_FIRST_DAY = 20250120
"""First date on which a MainTel/Simonyi message means LSSTCam.

Messages added before this date were about LSSTComCam.
"""


def _noon_utc(dayobs: int) -> str:
    """Noon UTC starting ``dayobs``, as a naive ISO timestamp."""
    return dt.datetime.strptime(str(dayobs), "%Y%m%d").replace(hour=12).isoformat()


class NarrativelogCachedAdapter(MutableDataMixin, RestCachedAdapter):
    """Fetches and caches Narrative Log messages per dayobs.

    Messages for **all telescopes** are cached together under one
    dayobs key, each with a derived ``instrument`` field — the service
    filters by instrument when collating. This keeps the cache key
    purely ``(adapter, dayobs)`` and means one upstream fetch (and one
    ``RefreshWorker`` refresh) covers every instrument.

    The upstream API takes a ``date_begin`` time window rather than a
    dayobs range, so each contiguous run of missing days is queried as
    noon UTC on the first day to noon UTC after the last day, and the
    messages are partitioned into dayobs by their ``date_begin``.
    """

    name = "narrativelog"

    def __init__(self, redis: Any, server_url: str | None = None, page_limit: int = 1000):
        super().__init__(redis, server_url=server_url)
        self._page_limit = page_limit

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, list[dict]]:
        results: dict[int, list[dict]] = {dayobs: [] for dayobs in dayobs_list}
        for run_start, run_end in contiguous_runs(dayobs_list):
            logger.debug(f"Fetching Narrative Log messages for dayobs {run_start}..{run_end}")
            messages = self._get_json_paged(
                f"{self.server}/narrativelog/messages",
                params={
                    "is_human": "either",
                    "is_valid": "true",
                    "order_by": "-date_begin",
                    "min_date_begin": _noon_utc(run_start),
                    "max_date_begin": _noon_utc(add_or_subtract_dayobs_days(run_end, 1)),
                },
                page_limit=self._page_limit,
            )
            for message in messages:
                self._add_instrument(message)
                dayobs = dayobs_at(dt.datetime.fromisoformat(message["date_begin"]))
                if dayobs in results:
                    results[dayobs].append(message)
        return results

    @staticmethod
    def _add_instrument(message: dict) -> None:
        """Set ``message["instrument"]`` from the telescope (side effect).

        The Narrative Log records which telescope a message concerns
        (``components_json``), not which instrument, but the dashboard
        reports by instrument. AuxTel maps to LATISS; MainTel/Simonyi
        maps to the LSSTCam-family camera mounted when the message was
        added. Anything else maps to None.
        """
        components = message.get("components_json") or {}
        telescope = components.get("name")
        if telescope == "AuxTel":
            instrument = "LATISS"
        elif telescope in {"MainTel", "Simonyi"}:
            day_added = int(message["date_added"][:10].replace("-", ""))
            instrument = "LSSTCam" if day_added >= LSSTCAM_FIRST_DAY else "LSSTComCam"
        else:
            instrument = None
        message["instrument"] = instrument


@functools.cache
def get_narrativelog_adapter() -> NarrativelogCachedAdapter:
    return NarrativelogCachedAdapter(get_redis_client())
