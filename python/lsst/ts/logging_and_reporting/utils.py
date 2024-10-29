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

# NOTE on day_obs vs dayobs:
# Throughout Rubin, and perhaps Astonomy in general, a single night
# of observering (both before and after midnight portions) is referred
# to using 'date_obs' or 'dateobs'.
# Generaly its used as a single word when refering to a TYPE
# and as two words when referring to a FIELD. But there are
# plenty exceptions.  Nonetheless, this is the convention we use.
# One word most of the time, two_words when its a field such as
# in a Database or API query string.


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


# Idea stoeln from
# https://github.com/lsst-sitcom/summit_utils/blob/0b3fd8795c9cca32f30cef0c37625c5d96804b74/python/lsst/summit/utils/efdUtils.py#L633  # noqa: E501
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


# dayobs int to dayobs string (YYYY-MM-DD)
def dayobs_str(dayobs: int) -> str:
    dos = str(dayobs)
    return f"{dos[0:4]}-{dos[4:6]}-{dos[6:8]}"  # "YYYY-MM-DD"


# dayobs str (YYYY-MM-DD) to dayobs int
def dayobs_int(dayobs: str) -> int:
    return int(dayobs.replace("-", ""))


# dayobs (str:YYYY-MM-DD or YYYYMMDD) to datetime.
# Allow TODAY, YESTERDAY, TOMORROW
# was: dos2dt
def get_datetime_from_dayobs_str(dayobs):
    match dayobs.lower():
        case "today":
            date = dt.datetime.now().date()
        case "yesterday":
            date = dt.datetime.now().date() - dt.timedelta(days=1)
        case "tomorrow":
            date = dt.datetime.now().date() + dt.timedelta(days=1)
        case _:
            no_dash = dayobs.replace("-", "")
            date = dt.datetime.strptime(no_dash, "%Y%m%d").date()
    return dt.datetime.combine(date, dt.time(12))


dayobs2dt = get_datetime_from_dayobs_str


def hhmmss(decimal_hours):
    hours = int(decimal_hours)
    minutes = int((decimal_hours * 60) % 60)
    seconds = int((decimal_hours * 3600) % 60)
    return f"{hours:d}:{minutes:02d}:{seconds:02d}"


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


# Servers we might use
class Server:
    summit = "https://summit-lsp.lsst.codes"
    usdf = "https://usdf-rsp-dev.slac.stanford.edu"
    tucson = "https://tucson-teststand.lsst.codes"


def wrap_dataframe_columns(df):
    def spacify(name):
        str(name).replace("_", " ")

    column_map = {colname: spacify(colname) for colname in df.columns}
    return df.rename(columns=column_map)
