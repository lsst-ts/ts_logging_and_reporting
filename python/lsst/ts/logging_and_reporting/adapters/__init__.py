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
"""Singleton getters for the cached upstream-source adapters."""

from .almanac import get_almanac_adapter
from .consdb_exposures import get_consdb_exposures_adapter
from .rubin_nights_dome import get_rubin_nights_dome_adapter
from .visit_overhead import get_visit_overhead_adapter

# Adapters the refresh worker keeps warm, in the order it refreshes
# them. The order is load-bearing: visit_overhead reads what
# consdb_exposures cached, so it has to run after it. Do not sort this
# list to match `__all__`.
REFRESH_ADAPTERS = (
    get_consdb_exposures_adapter,
    get_visit_overhead_adapter,
    get_rubin_nights_dome_adapter,
)

__all__ = [
    "REFRESH_ADAPTERS",
    "get_almanac_adapter",
    "get_consdb_exposures_adapter",
    "get_rubin_nights_dome_adapter",
    "get_visit_overhead_adapter",
]
