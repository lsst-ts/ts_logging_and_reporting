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

import logging
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any

from lsst.ts.logging_and_reporting.almanac import Almanac

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


def get_almanac(dayobs_start: int, dayobs_end: int) -> list[dict[str, Any]]:
    """Return almanac records for a dayobs range.

    Parameters
    ----------
    dayobs_start : int
        Inclusive lower bound of the requested dayobs range.
    dayobs_end : int
        Exclusive upper bound of the requested dayobs range.

    Returns
    -------
    list[dict[str, Any]]
        Almanac records containing twilight, moon, and night-duration
        fields, plus ``elapsed_twilight_hours`` for each returned
        night.

    Notes
    -----
    The returned records are labeled by the dayobs of the morning
    twilight boundary, so querying ``[dayobs_start, dayobs_end)`` yields
    records whose ``dayobs`` values span ``dayobs_start + 1`` through
    ``dayobs_end``.
    """
    logger.info(f"Getting almanac for start: {dayobs_start}, end: {dayobs_end}")
    try:
        # adding one day to the start and end dates as the Almanac adapter
        # considers only max_dayobs, which is exclused from the dayobs range
        start = datetime.strptime(str(dayobs_start), "%Y%m%d") + timedelta(days=1)
        end = datetime.strptime(str(dayobs_end), "%Y%m%d") + timedelta(days=1)
        almanac_info = []
        current = start
        while current < end:
            dayobs = int(current.strftime("%Y%m%d"))
            almanac = Almanac(min_dayobs=dayobs_start, max_dayobs=dayobs)
            night_events = almanac.as_dict[0]
            twilight_evening_12deg = night_events["Evening Nautical Twilight"]
            twilight_morning_12deg = night_events["Morning Nautical Twilight"]
            almanac_info.append(
                {
                    "dayobs": dayobs,
                    "night_hours": almanac.night_hours,
                    "twilight_evening_18deg": night_events["Evening Astronomical Twilight"],
                    "twilight_morning_18deg": night_events["Morning Astronomical Twilight"],
                    "twilight_evening_12deg": twilight_evening_12deg,
                    "twilight_morning_12deg": twilight_morning_12deg,
                    "twilight_evening_6deg": night_events["Evening Civil Twilight"],
                    "twilight_morning_6deg": night_events["Morning Civil Twilight"],
                    "twilight_evening_0deg": night_events["Sun Set"],
                    "twilight_morning_0deg": night_events["Sun Rise"],
                    "moon_rise_time": night_events["Moon Rise"],
                    "moon_set_time": night_events["Moon Set"],
                    "moon_illumination": night_events["Moon Illumination"],
                    "elapsed_twilight_hours": _compute_elapsed_twilight_hours(
                        almanac.night_hours,
                        twilight_evening_12deg,
                        twilight_morning_12deg,
                    ),
                }
            )
            current += timedelta(days=1)
        return almanac_info
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error fetching almanac data for: {dayobs_start}, {dayobs_end}. Error: {e}")
        raise e
