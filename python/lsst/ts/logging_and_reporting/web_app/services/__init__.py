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

"""Singleton getters for the endpoint services."""

from .almanac import get_almanac_service
from .block_details import get_block_details_service
from .data_log import get_data_log_service
from .expected_exposures import get_expected_exposures_service
from .exposure_entries import get_exposure_entries_service
from .exposure_flags import get_exposure_flags_service
from .exposures import get_exposures_service
from .jira import get_jira_tickets_service
from .narrativelog_service import get_narrative_log_service
from .nightreport_service import get_night_report_service

__all__ = [
    "get_almanac_service",
    "get_block_details_service",
    "get_data_log_service",
    "get_expected_exposures_service",
    "get_exposure_entries_service",
    "get_exposure_flags_service",
    "get_exposures_service",
    "get_jira_tickets_service",
    "get_narrative_log_service",
    "get_night_report_service",
]
