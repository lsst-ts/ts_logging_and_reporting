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

import logging

import lsst.ts.logging_and_reporting.utils as nd_utils
from lsst.ts.logging_and_reporting.source_adapters import NightReportAdapter

logger = logging.getLogger(__name__)


def get_night_reports(dayobs_start: int, dayobs_end: int, auth_token: str = None) -> list:
    """Get nightreport records for a given time range."""
    logger.info(f"Getting night reports for start: {dayobs_start}, end: {dayobs_end}")
    nightreport = NightReportAdapter(
        server_url=nd_utils.Server.get_url(),
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
        auth_token=auth_token,
    )
    status = nightreport.get_records()
    logger.debug(f"status: {status}")
    if status.get("error") is not None:
        raise Exception(f"Error getting nightreport records from {status.endpoint_url}: {status.error}")
    return nightreport.records
