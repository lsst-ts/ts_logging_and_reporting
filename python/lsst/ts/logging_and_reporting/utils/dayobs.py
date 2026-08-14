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
"""Dayobs and date conversion helpers.

A dayobs is the noon-to-noon UTC observing day, in ``YYYYMMDD`` integer
form.
"""

import datetime as dt

import pandas as pd
import pytz


def current_dayobs_utc(now_utc: pd.Timestamp | dt.datetime) -> int:
    """Compute the active dayobs for a UTC timestamp.

    Parameters
    ----------
    now_utc : pandas.Timestamp or datetime.datetime
        UTC timestamp to convert.

    Returns
    -------
    int
        Dayobs in ``YYYYMMDD`` form.

    Notes
    -----
    A dayobs runs from noon UTC to noon UTC, so subtracting 12 hours
    and taking the date gives the correct dayobs for any time in that
    window.
    """
    return int((now_utc - dt.timedelta(hours=12)).strftime("%Y%m%d"))


def dayobs_int(dayobs: str) -> int:
    """Convert a ``YYYY-MM-DD`` dayobs string to a YYYYMMDD integer."""
    return int(str(dayobs).replace("-", ""))


def get_utc_datetime_from_dayobs_str(dayobs):
    """Convert a dayobs string to an UTC datetime object
    at noon (start of observing day).

    Parameters
    ----------
    dayobs : `str` | `int`
        The dayobs string in YYYY-MM-DD or YYYYMMDD.

    Returns
    -------
    datetime : `datetime.datetime`
        The corresponding UTC datetime at noon (start of observing day).
    """
    no_dash_dayobs = str(dayobs).replace("-", "")
    datetime = dt.datetime.strptime(no_dash_dayobs, "%Y%m%d").replace(
        hour=12, minute=0, second=0, tzinfo=pytz.UTC
    )
    return datetime


def add_or_subtract_dayobs_days(dayobs: int, days: int) -> int:
    """Add or subtract a specified number of days to a
    YYYYMMDD dayobs integer.
    """
    date = dt.datetime.strptime(str(dayobs), "%Y%m%d")
    new_date = date + dt.timedelta(days=days)
    return int(new_date.strftime("%Y%m%d"))
