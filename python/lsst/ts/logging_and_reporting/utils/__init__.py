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

"""Utility helpers, grouped by concern into submodules.

New code should import from the specific concern module — ``utils.dayobs``,
``utils.auth``, ``utils.serialization``, ``utils.collation``, ``utils.misc``.
The names are re-exported here so ``from ...utils import X`` and
``import ...utils as ut`` continue to resolve while the remaining callers
of that flat form are migrated.
"""

from lsst.ts.logging_and_reporting.utils.auth import (
    AUTH_SOURCES,
    Server,
    get_access_token,
    get_auth_header,
    get_jira_hostname,
    retrieve_access_token,
)
from lsst.ts.logging_and_reporting.utils.collation import flatten_sorted
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    contiguous_runs,
    current_dayobs,
    date_to_dayobs_int,
    datetime_to_dayobs,
    dayobs2dt,
    dayobs_at,
    dayobs_int,
    dayobs_int_to_date,
    dayobs_range,
    dayobs_str,
    get_utc_datetime_from_dayobs_str,
)
from lsst.ts.logging_and_reporting.utils.misc import (
    JIRA_BLOCK_BASE_URL,
    ZEPHYR_BLOCK_BASE_URL,
    DatetimeIter,
    Timer,
    build_block_response,
    date_hr_min,
    fallback_parameters,
    hhmmss,
    tic,
    toc,
)
from lsst.ts.logging_and_reporting.utils.serialization import make_json_safe, stringify_special_floats

__all__ = [
    "AUTH_SOURCES",
    "DatetimeIter",
    "JIRA_BLOCK_BASE_URL",
    "Server",
    "Timer",
    "ZEPHYR_BLOCK_BASE_URL",
    "add_or_subtract_dayobs_days",
    "build_block_response",
    "contiguous_runs",
    "current_dayobs",
    "date_hr_min",
    "date_to_dayobs_int",
    "datetime_to_dayobs",
    "dayobs2dt",
    "dayobs_at",
    "dayobs_int",
    "dayobs_int_to_date",
    "dayobs_range",
    "dayobs_str",
    "fallback_parameters",
    "flatten_sorted",
    "get_access_token",
    "get_auth_header",
    "get_jira_hostname",
    "get_utc_datetime_from_dayobs_str",
    "hhmmss",
    "make_json_safe",
    "retrieve_access_token",
    "stringify_special_floats",
    "tic",
    "toc",
]
