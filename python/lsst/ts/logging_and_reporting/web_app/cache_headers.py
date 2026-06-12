# This file is part of ts_logging_and_reporting.
#
# Developed for the LSST Telescope and Site Systems.
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

from datetime import datetime, timedelta

from fastapi import Response

# Cache-Control header values
CACHE_SHORT = "public, max-age=60"  # 1 minute - for current day or dynamic data
CACHE_LONG = "public, max-age=86400"  # 1 day - for historical immutable data


def is_current_dayobs(dayobs_start: int, dayobs_end: int) -> bool:
    """Check if the current observing day falls within the given dayobs range.

    Parameters
    ----------
    dayobs_start : `int`
        Start observation date in YYYYMMDD format.
    dayobs_end : `int`
        End observation date in YYYYMMDD format.

    Returns
    -------
    `bool`
        True if today's dayobs is within the range (inclusive), False otherwise
    """
    # Get current UTC datetime, adjust for observing day (starts at noon)
    now_utc = datetime.utcnow()
    current_dayobs_dt = now_utc - timedelta(hours=12)
    current_dayobs = int(current_dayobs_dt.strftime("%Y%m%d"))

    # Ensure start <= end
    if dayobs_start > dayobs_end:
        dayobs_start, dayobs_end = dayobs_end, dayobs_start

    return dayobs_start <= current_dayobs <= dayobs_end


def get_cache_control_header(dayobs_start: int, dayobs_end: int, always_short: bool = False) -> str:
    """Determine Cache-Control header based on whether the request includes
    current dayobs.

    Parameters
    ----------
    dayobs_start : `int`
        Start observation date in YYYYMMDD format.
    dayobs_end : `int`
        End observation date in YYYYMMDD format.
    always_short : `bool`, optional
        If True, always return short cache regardless of dayobs.
        Use for data that can change (e.g., Jira tickets).

    Returns
    -------
    `str`
        Cache-Control header value.
    """
    if always_short or is_current_dayobs(dayobs_start, dayobs_end):
        return CACHE_SHORT
    else:
        return CACHE_LONG


def apply_cache_headers(
    response: Response, dayobs_start: int, dayobs_end: int, always_short: bool = False
) -> Response:
    """Apply cache control headers to a response.

    Parameters
    ----------
    response : `Response`
        FastAPI response object.
    dayobs_start : `int`
        Start observation date in YYYYMMDD format.
    dayobs_end : `int`
        End observation date in YYYYMMDD format.
    always_short : `bool`, optional
        If True, always return short cache regardless of dayobs.
        Use for data that can change (e.g., Jira tickets).

    Returns
    -------
    `Response`
        Response with cache headers applied.
    """
    cache_control = get_cache_control_header(dayobs_start, dayobs_end, always_short)
    response.headers["Cache-Control"] = cache_control
    return response
