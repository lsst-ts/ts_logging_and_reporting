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
import math
import os
import time

import numpy as np
import pandas as pd
import pytz
from fastapi import HTTPException, Request

# NOTE on day_obs vs dayobs:
# Throughout Rubin, and perhaps Astonomy in general, a single night
# of observering (both before and after midnight portions) is referred
# to using 'date_obs' or 'dateobs'.
# Generaly its used as a single word when refering to a TYPE
# and as two words when referring to a FIELD. But there are
# plenty exceptions.  Nonetheless, this is the convention we use.
# One word most of the time, two_words when its a field such as
# in a Database or API query string.


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


# Idea stolen from
# https://tinyurl.com/efdutilsL633
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
    return int(str(dayobs).replace("-", ""))


# dayobs (str:YYYY-MM-DD or YYYYMMDD) to datetime.
# Allow TODAY, YESTERDAY, TOMORROW
# was: dos2dt
def get_utc_datetime_from_dayobs_str(dayobs):
    # Add timezone = Chile to now datetime
    dome_tz = pytz.timezone("Chile/Continental")
    dome_today_noon = dome_tz.localize(dt.datetime.now().replace(hour=12, minute=0, second=0, microsecond=0))

    dayobs_str = str(dayobs)
    match dayobs_str.lower():
        case "today":
            datetime = dome_today_noon
        case "yesterday":
            datetime = dome_today_noon - dt.timedelta(days=1)
        case "tomorrow":
            datetime = dome_today_noon + dt.timedelta(days=1)
        case _:
            no_dash = dayobs_str.replace("-", "")
            # noon is the start of an observing day
            datetime = dome_tz.localize(
                dt.datetime.strptime(no_dash, "%Y%m%d").replace(hour=12, minute=0, second=0)
            )
    return datetime.astimezone(pytz.utc)


dayobs2dt = get_utc_datetime_from_dayobs_str


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


# Servers we might use
class Server:
    """Do I need a class for this instead of just using line 262?"""

    summit = "https://summit-lsp.lsst.codes"
    usdfdev = "https://usdf-rsp-dev.slac.stanford.edu"
    usdf = "https://usdf-rsp.slac.stanford.edu"
    tucson = "https://tucson-teststand.lsst.codes"
    base = "https://base-lsp.lsst.codes"

    @classmethod
    def get_all(cls):
        return [
            value for value in cls.__dict__.values() if isinstance(value, str) and value.startswith("https")
        ]

    @classmethod
    def get_url(cls):
        env_var_name = "EXTERNAL_INSTANCE_URL"
        current = os.environ.get(env_var_name)

        match current:
            case Server.summit:
                return Server.summit
            case Server.usdfdev:
                return Server.usdfdev
            case Server.usdf:
                return Server.usdf
            case Server.tucson:
                return Server.tucson
            case Server.base:
                return Server.base
            case _:
                raise ValueError(f"Unset or invalid {env_var_name}: {current}")


def wrap_dataframe_columns(df):
    def spacify(name):
        return str(name).replace("_", " ")

    column_map = {colname: spacify(colname) for colname in df.columns}
    return df.rename(columns=column_map)


def get_access_token(request: Request = None):
    """Return access token to be sent in headers as Auth Bearer

    When calling from a notebook on the RSP `lsst.rsp.utils.get_access_token`
    is used to get the token from the active client session.

    When called from the FastAPI web server in local development
    the token is read from the `ACCESS_TOKEN` environment variable.

    Otherwise we assume this is called from the FastAPI web server running
    on the RSP and the token is read from the request headers.

    Parameters
    ----------
    request : `fastapi.Request`, optional
        The request object, if available. Used to
        extract the token from headers.

    Raises
    ------
    HTTPException
        If the access token cannot be retrieved by any method.

    Returns
    -------
    str or None
        The access token if available, otherwise None.
    """
    try:
        import lsst.rsp.utils

        return lsst.rsp.utils.get_info()
    except ImportError:
        env_token = os.getenv("ACCESS_TOKEN")
        if env_token is not None:
            return env_token

        if request is not None:
            auth_header = request.headers.get("Authorization")
            if auth_header is not None and " " in auth_header:
                return auth_header.split(" ")[1]

    raise HTTPException(
        status_code=401, detail="RSP authentication token could not be retrieved by any method."
    )


def get_auth_header(token=None):
    """return dict obj for request auth headers"""
    bearer_token = token if token is not None else get_access_token()
    return {"Authorization": f"Bearer {bearer_token}"}


def stringify_special_floats(val):
    """
    Convert special float values into JSON-safe string representations.

    This function ensures that pandas DataFrames containing NaN,
    positive infinity, or negative infinity values can be safely
    serialized to JSON without causing errors.

    Parameters
    ----------
    val : any
        The value to check and possibly convert.

    Returns
    -------
    any
        - "NaN" if the value is NaN
        - "Infinity" if the value is positive infinity
        - "-Infinity" if the value is negative infinity
        - The original value otherwise
    """
    if isinstance(val, float):
        if np.isnan(val):
            return "NaN"
        elif np.isposinf(val):
            return "Infinity"
        elif np.isneginf(val):
            return "-Infinity"
    return val


def make_json_safe(obj):
    """
    Recursively converts objects to be JSON serializable.

    This function traverses the input object, converting
    any non-JSON-serializable types (such as Astropy Time objects,
    NumPy integers, floats, NaN, or infinity) into types
    that can be safely serialized.
    Dictionaries and lists are processed recursively. NaN and infinity values
    are replaced with None.

    Parameters
    ----------
    obj : any
        The object to convert. Can be a dict, list, or any value.

    Returns
    -------
    any
        The converted object, safe for JSON serialization.
    """
    if obj is None or isinstance(obj, (bool, str)):
        return obj

    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        result = [make_json_safe(v) for v in obj]
        return tuple(result) if isinstance(obj, tuple) else result

    # Check for Astropy Time BEFORE NumPy array check
    obj_type_name = type(obj).__name__
    if obj_type_name == "Time" and hasattr(obj, "to_datetime"):
        dt = obj.to_datetime()
        # Handle both scalar and array Time objects
        if isinstance(dt, np.ndarray):
            return [make_json_safe(v) for v in dt]
        return dt.isoformat()

    if isinstance(obj, np.ndarray):
        if obj.ndim == 0:
            return make_json_safe(obj.item())
        return [make_json_safe(v) for v in obj.tolist()]

    if obj is pd.NaT or obj is pd.NA:
        return None

    if isinstance(obj, pd.Timestamp):
        if pd.isnull(obj):
            return None
        return obj.isoformat()

    if isinstance(obj, np.datetime64):
        if pd.isnull(obj):
            return None
        return pd.Timestamp(obj).isoformat()

    if isinstance(obj, (pd.Timedelta, np.timedelta64)):
        return float(pd.Timedelta(obj).total_seconds())

    if isinstance(obj, np.bool_):
        return bool(obj)

    if isinstance(obj, np.integer):
        return int(obj)

    if isinstance(obj, np.floating):
        v = float(obj)
        return None if (np.isnan(v) or np.isinf(v)) else v

    if isinstance(obj, int):
        return obj

    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj

    return obj
