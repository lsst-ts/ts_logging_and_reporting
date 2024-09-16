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


import time
import datetime


# See https://github.com/lsst-sitcom/summit_utils/blob/0b3fd8795c9cca32f30cef0c37625c5d96804b74/python/lsst/summit/utils/efdUtils.py#L633
def datetime_to_dayobs(dt) -> int:
    """Convert a datetime object to dayobs.
    Round to the date of the start of the observing night.
    Both the input datetime and output dayobs are in the same timezone.

    Parameters
    ----------
    dt : `datetime.datetime`
        The date-time.

    Returns
    -------
    day_obs : `int`
        The day_obs, as an integer, e.g. 20231225 (YYYYMMDD)
    """
    return (dt - datetime.timedelta(hours=12)).date()



def tic():
    """Start timer.
    """
    tic.start = time.perf_counter()

def toc():
    """Stop timer.

    Returns
    -------
    elapsed_seconds : float
       Elapsed time in fractional seconds since the previous `tic()`.
    """

    elapsed_seconds = time.perf_counter() - tic.start
    return elapsed_seconds # fractional

class Timer():
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
        return elapsed_seconds # fractional
