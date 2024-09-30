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

import datetime as dt
import time


class datetime_iter:
    def __init__(self, start_datetime, stop_datetime, increment=None):
        """increment:: datetime.timedelta"""
        self.start_datetime = start_datetime
        self.stop_datetime = stop_datetime
        self.increment = increment if increment else dt.timedelta(days=1)
        self.increasing = self.increment.total_seconds() >= 0

    def __iter__(self):
        self.date = self.start_datetime
        return self

    def __next__(self):

        if self.increasing:
            not_done = self.date <= self.stop_datetime
        else:
            not_done = self.date >= self.stop_datetime
        if not_done:
            # INCLUSIVE
            date = self.date
            self.date += self.increment
            return date
        else:
            raise StopIteration


# See https://github.com/lsst-sitcom/summit_utils/blob/0b3fd8795c9cca32f30cef0c37625c5d96804b74/python/lsst/summit/utils/efdUtils.py#L633  # noqa: E501
# was: datetime_to_dayobs   # TODO remove
def datetime_to_day_obs(datetime) -> str:
    """Convert a datetime object to day_obs.
    Round to the date of the start of the observing night.
    Both the input datetime and output dayobs are in the same timezone.
    Format of dayobs is

    Parameters
    ----------
    datetime : `datetime.datetime`
        The date-time.

    Returns
    -------
    day_obs : `str`
        The day_obs, as a strung, e.g. 2023-12-25 (YYYY-MM-DD)
    """
    if isinstance(datetime, dt.datetime):
        dodate = (datetime - dt.timedelta(hours=12)).date()
    else:
        dodate = datetime
    return dodate.strftime("%Y-%m-%d")


# day_obs int to day_obs string (YYYY-MM-DD)
def day_obs_str(day_obs: int) -> str:
    dos = str(day_obs)
    return f"{dos[0:4]}-{dos[4:6]}-{dos[6:8]}"  # "YYYY-MM-DD"


# day_obs str (YYYY-MM-DD) to day_obs int
def day_obs_int(day_obs: str) -> int:
    return int(day_obs.replace("-", ""))


# day_obs (str:YYYY-MM-DD or YYYYMMDD) to datetime.
# Allow TODAY, YESTERDAY, TOMORROW
# was: dos2dt
def get_datetime_from_day_obs_str(day_obs):
    match day_obs.lower():
        case "today":
            date = dt.datetime.now().date()
        case "yesterday":
            date = dt.datetime.now().date() - dt.timedelta(days=1)
        case "tomorrow":
            date = dt.datetime.now().date() + dt.timedelta(days=1)
        case _:
            no_dash = day_obs.replace("-", "")
            date = dt.datetime.strptime(no_dash, "%Y%m%d").date()
    return date


def tic():
    """Start timer."""
    tic.start = time.perf_counter()


def toc():
    """Stop timer.

    Returns
    -------
    elapsed_seconds : float
       Elapsed time in fractional seconds since the previous `tic()`.
    """

    elapsed_seconds = time.perf_counter() - tic.start
    return elapsed_seconds  # fractional


class Timer:
    """Elapsed seconds timer.

    Multiple instances can be used simultaneously and can overlap.
    Repeated use of `toc` without an intervening `tic` will yield increasing
    large elapsed times starting from the same point in time.

    Example:
       timer0 = Timer()
       ...do stuff...
       timer1 = Timer()
       ...do stuff...
       elapsed1 = timer1.toc        # 10.1
       ...do stuff...
       elapsed1bigger = timer1.toc  # 22.1
       elapsed0 = timer0.toc        # 50.0
    """

    def __init__(self):
        self.tic

    @property
    def tic(self):
        self.start = time.perf_counter()
        return self.start

    @property
    def toc(self):
        elapsed_seconds = time.perf_counter() - self.start
        return elapsed_seconds  # fractional
