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

"""Dayobs and date conversion helpers.

A dayobs is the noon-to-noon UTC observing day, in ``YYYYMMDD`` integer
form. These helpers convert between dayobs, dates, and timestamps, and
enumerate dayobs ranges.
"""

import datetime as dt

import pandas as pd
import pytz


def dayobs_at(time_utc: pd.Timestamp | dt.datetime) -> int:
    """Compute the active dayobs for a UTC timestamp.

    Parameters
    ----------
    time_utc : pandas.Timestamp or datetime.datetime
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
    return int((time_utc - dt.timedelta(hours=12)).strftime("%Y%m%d"))


def current_dayobs() -> int:
    """The current astronomical dayobs (noon-to-noon UTC)."""
    return dayobs_at(dt.datetime.now(dt.timezone.utc))


def datetime_to_dayobs(datetime) -> str:
    """Convert a datetime object to dayobs.
    Round to the date of the start of the observing night.
    Both the input datetime and output dayobs are in the same timezone.
    Format of dayobs is

    Parameters
    ----------
    datetime : `datetime.datetime`
        The date-time.

    Returns
    -------
    dayobs : `str`
        The dayobs, as a string, e.g. 2023-12-25 (YYYY-MM-DD)
    """
    if isinstance(datetime, dt.datetime):
        dodate = (datetime - dt.timedelta(hours=12)).date()
    else:
        dodate = datetime
    return dodate.strftime("%Y-%m-%d")


def dayobs_str(dayobs: int) -> str:
    """Convert a YYYYMMDD dayobs integer to a ``YYYY-MM-DD`` string."""
    dos = str(dayobs)
    return f"{dos[0:4]}-{dos[4:6]}-{dos[6:8]}"


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


dayobs2dt = get_utc_datetime_from_dayobs_str


def add_or_subtract_dayobs_days(dayobs: int, days: int) -> int:
    """Add or subtract a specified number of days to a
    YYYYMMDD dayobs integer.
    """
    date = dt.datetime.strptime(str(dayobs), "%Y%m%d")
    new_date = date + dt.timedelta(days=days)
    return int(new_date.strftime("%Y%m%d"))


def dayobs_int_to_date(dayobs: int) -> dt.date:
    """Convert a YYYYMMDD dayobs integer to a `datetime.date`."""
    return dt.datetime.strptime(str(dayobs), "%Y%m%d").date()


def date_to_dayobs_int(date: dt.date) -> int:
    """Convert a `datetime.date` to a YYYYMMDD dayobs integer."""
    return int(date.strftime("%Y%m%d"))


def dayobs_range(start_dayobs: int, end_dayobs: int) -> list[int]:
    """Enumerate all dayobs in ``[start_dayobs, end_dayobs]`` inclusive.

    Parameters
    ----------
    start_dayobs : `int`
        First dayobs of the range, in YYYYMMDD form.
    end_dayobs : `int`
        Last dayobs of the range (inclusive), in YYYYMMDD form.

    Returns
    -------
    `list` [`int`]
        Every dayobs in the range, ascending.
    """
    start = dayobs_int_to_date(start_dayobs)
    end = dayobs_int_to_date(end_dayobs)
    days = []
    while start <= end:
        days.append(date_to_dayobs_int(start))
        start += dt.timedelta(days=1)
    return days


def contiguous_runs(dayobs_list: list[int]) -> list[tuple[int, int]]:
    """Group dayobs into contiguous ``(start, end)`` runs (inclusive).

    Adjacency is calendar-aware (20250131 and 20250201 are adjacent).

    Parameters
    ----------
    dayobs_list : `list` [`int`]
        Dayobs in YYYYMMDD form, in any order.

    Returns
    -------
    `list` [`tuple` [`int`, `int`]]
        Inclusive ``(start_dayobs, end_dayobs)`` per run, ascending.
        e.g. ``[20250101, 20250103, 20250104]`` →
        ``[(20250101, 20250101), (20250103, 20250104)]``.
    """
    if not dayobs_list:
        return []
    dates = sorted(dayobs_int_to_date(d) for d in set(dayobs_list))
    runs = []
    run_start = run_end = dates[0]
    for date in dates[1:]:
        if date - run_end == dt.timedelta(days=1):
            run_end = date
        else:
            runs.append((date_to_dayobs_int(run_start), date_to_dayobs_int(run_end)))
            run_start = run_end = date
    runs.append((date_to_dayobs_int(run_start), date_to_dayobs_int(run_end)))
    return runs
