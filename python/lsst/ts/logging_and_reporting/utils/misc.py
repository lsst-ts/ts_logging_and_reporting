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

"""Assorted helpers awaiting a home or removal.

BLOCK-link building, timers, datetime iteration, and small formatting
helpers that do not (yet) belong to a dedicated concern module.
"""

import datetime as dt
import os
import time

import pandas as pd

from lsst.ts.logging_and_reporting.utils.dayobs import get_utc_datetime_from_dayobs_str

# Base urls for BLOCK links
ZEPHYR_BLOCK_BASE_URL = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/projects/BLOCK?selectedItem=com.atlassian.plugins.atlassian-connect-plugin:com.kanoah.test-manager__main-project-page#!/v2/testCase/"
JIRA_BLOCK_BASE_URL = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/browse/"


def date_hr_min(iso_dt_str):
    # return YYYY-MM-DD HH:MM
    return str(dt.datetime.fromisoformat(iso_dt_str))[:16]


def fallback_parameters(day_obs, number_of_days, period, verbose, warning):
    """Given parameters from Times Square, return usable versions of
    all parameters.  If the provide parameters are not usable, return
    default usable ones.
    """
    day_obs_default = "YESTERDAY"  # Fall Back value
    days_default = 1
    period_default = "4h"
    message = ""

    try:
        # dayobs(str): YYYY-MM-DD, YYYYMMDD, TODAY, YESTERDAY
        get_utc_datetime_from_dayobs_str(day_obs)  # ignore result
    except Exception as err:
        message += f"""\nInvalid day_obs given: {day_obs!r}
        Available values are: YYYY-MM-DD, YYYYMMDD, TODAY, YESTERDAY.
        Using: {day_obs_default!r}\n{str(err)!r}
        """
        day_obs = day_obs_default

    try:
        days = int(number_of_days)
    except Exception as err:
        days = days_default
        message += f"""\nInvalid number_of_days given: {number_of_days!r}
        Must be an integer.
        Using: {days}\n{str(err)!r}
        """

    try:
        now = dt.datetime.now()
        freq = pd.Period(now, period).freqstr
    except Exception as err:
        freq = period_default
        message += f"\nInvalid period given: {period!r}\n"
        message += "Must be an Alias string formed from "
        message += "https://pandas.pydata.org/docs/user_guide/timeseries.html#period-aliases"
        message += f"Using: {freq}\n{str(err)!r}"

    to_use = dict(
        day_obs=day_obs,
        number_of_days=days,
        period=freq,
        verbose=(verbose == "true"),
        warning=(warning == "true"),
    )

    return to_use, message


class DatetimeIter:
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


def hhmmss(decimal_hours):
    if pd.isna(decimal_hours):
        return decimal_hours

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


def build_block_response(zephyr_data, jira_data):
    """Construct a unified response object for BLOCK details.

    Combines Zephyr and Jira BLOCK data into a single dictionary keyed by
    BLOCK identifier. Each entry includes the key, summary, source, and a
    correctly formatted URL. Zephyr BLOCK URLs are derived from the parent
    key (i.e., suffixes after "_" are removed), while Jira BLOCK URLs use
    the full key.

    Parameters
    ----------
    zephyr_data : dict
        Mapping of Zephyr BLOCK keys to summaries.
    jira_data : dict
        Mapping of Jira BLOCK keys to summaries.

    Returns
    -------
    dict
        A dictionary mapping each BLOCK key to a JSON-serializable object
        containing:
        - "key": str
        - "summary": str
        - "source": "zephyr" or "jira"
        - "url": str
    """
    result = {}

    for key, summary in zephyr_data.items():
        # Test cases with _# at the end are represented in
        # Zephyr Scale without the _# at the end.
        parent_key = key.split("_", 1)[0]
        result[key] = {
            "key": key,
            "summary": summary,
            "source": "zephyr",
            "url": f"{ZEPHYR_BLOCK_BASE_URL}{parent_key}",
        }

    for key, summary in jira_data.items():
        result[key] = {
            "key": key,
            "summary": summary,
            "source": "jira",
            "url": f"{JIRA_BLOCK_BASE_URL}{key}",
        }

    return result
