import logging

import lsst.ts.logging_and_reporting.utils as nd_utils
from lsst.ts.logging_and_reporting.source_adapters import NarrativelogAdapter

logger = logging.getLogger(__name__)


def get_messages(dayobs_start: int, dayobs_end: int, telescope: str, auth_token: str = None) -> list:
    """
    Get messages from the narrative log for a given time range and telescope.
    """
    logger.info(
        f"Getting narrative log messages for start: {dayobs_start}, "
        f"end: {dayobs_end} and telescope: {telescope}"
    )
    narrative_log = NarrativelogAdapter(
        server_url=nd_utils.Server.get_url(),
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
        auth_token=auth_token,
    )
    status = narrative_log.get_records()
    logger.debug(f"status: {status}")
    if status.get("error") is not None:
        raise Exception(f"Error getting narrative log records from {status.endpoint_url}: {status.error}")
    records = narrative_log.records
    instrument_records = [record for record in records if record.get("instrument") == telescope]
    return instrument_records
