import datetime
from lsst.ts.logging_and_reporting.source_adapters import NarrativelogAdapter
import lsst.ts.logging_and_reporting.utils as nd_utils


def get_messages(dayobs_start: datetime.date, dayobs_end: datetime.date, telescope: str) -> list:
    """
    Get messages from the narrative log for a given time range and telescope.
    """
    try:
        narrative_log = NarrativelogAdapter(
            server_url=nd_utils.Server.get_url(),
            # max_dayobs=dayobs_end,
            # min_dayobs=dayobs_start,
        )
        status = narrative_log.get_records()
        print(status)
        records = narrative_log.records
        instrument_records = [record for record in records if record.get("instrument") == telescope]
        return instrument_records
    except Exception as e:
        print(f"Error getting narrative log records: {e}")
        return []
