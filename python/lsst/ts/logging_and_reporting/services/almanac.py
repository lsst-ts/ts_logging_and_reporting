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

"""Service for the /almanac endpoint.

The cached records hold only deterministic ephemeris data; the
time-dependent ``elapsed_twilight_hours`` field is computed here at
collation time so it is always current.
"""

import functools
import logging
from datetime import UTC, datetime
from typing import Any

from lsst.ts.logging_and_reporting.adapters.almanac import get_almanac_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils import add_or_subtract_dayobs_days

logger = logging.getLogger(__name__)


def _as_utc_datetime(timestamp: str) -> datetime:
    """Parse an almanac timestamp as a timezone-aware UTC datetime.

    Parameters
    ----------
    timestamp : str
        ISO-format timestamp string returned by the almanac adapter.

    Returns
    -------
    datetime.datetime
        Timestamp converted to UTC. Naive inputs are assumed to already
        represent UTC.
    """
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _compute_elapsed_twilight_hours(
    night_hours: float,
    twilight_evening_12deg: str,
    twilight_morning_12deg: str,
    now_utc: datetime | None = None,
) -> float:
    """Compute completed nautical-twilight hours for a night.

    Parameters
    ----------
    night_hours : float
        Total duration of the night, in hours.
    twilight_evening_12deg : str
        Evening 12-degree twilight timestamp in ISO format.
    twilight_morning_12deg : str
        Morning 12-degree twilight timestamp in ISO format.
    now_utc : datetime.datetime or None, optional
        Reference UTC timestamp. If not provided, the current UTC time
        is used.

    Returns
    -------
    float
        Completed twilight hours, clamped to the inclusive range
        ``[0, night_hours]``.

    Notes
    -----
    Future nights contribute 0 hours, completed nights contribute the full
    ``night_hours``, and an in-progress night contributes the elapsed time
    since evening nautical twilight.
    """
    now_utc = now_utc or datetime.now(UTC)
    evening_twilight_utc = _as_utc_datetime(twilight_evening_12deg)
    morning_twilight_utc = _as_utc_datetime(twilight_morning_12deg)

    if now_utc <= evening_twilight_utc:
        return 0.0
    if now_utc >= morning_twilight_utc:
        return night_hours

    elapsed_hours = (now_utc - evening_twilight_utc).total_seconds() / 3600
    return min(elapsed_hours, night_hours)


class AlmanacService(Service):
    """Collates per-night almanac records for /almanac."""

    def handle_request(self, day_obs_start: int, day_obs_end: int) -> dict:
        """Return almanac records for the dayobs range.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the API
            contract — the frontend sends end + 1 day).

        Notes
        -----
        Almanac records are labeled by the dayobs of the morning
        twilight boundary, so the range yields records labeled
        ``day_obs_start + 1`` through ``day_obs_end``.
        """
        per_day = self.adapters["almanac"].fetch(add_or_subtract_dayobs_days(day_obs_start, 1), day_obs_end)
        response = self.collate_response(per_day)
        logger.debug(
            f"Collated {len(response['almanac_info'])} almanac records "
            f"for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end}"
        )
        return response

    def collate_response(self, data: dict[int, Any]) -> dict:
        records = [data[dayobs] for dayobs in sorted(data)]
        now_utc = datetime.now(UTC)
        for record in records:
            record["elapsed_twilight_hours"] = _compute_elapsed_twilight_hours(
                record["night_hours"],
                record["twilight_evening_12deg"],
                record["twilight_morning_12deg"],
                now_utc,
            )
        return {"almanac_info": records}


@functools.cache
def get_almanac_service() -> AlmanacService:
    return AlmanacService(adapters={"almanac": get_almanac_adapter()})
