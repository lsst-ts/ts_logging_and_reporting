import logging
from lsst.ts.logging_and_reporting.source_adapters import NightReportAdapter
import lsst.ts.logging_and_reporting.utils as nd_utils


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
