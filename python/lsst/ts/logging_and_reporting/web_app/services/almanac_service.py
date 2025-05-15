import datetime
import logging
from lsst.ts.logging_and_reporting.almanac import Almanac

logger = logging.getLogger(__name__)


def get_almanac(dayobs_start: datetime.date, dayobs_end: datetime.date) -> list:
    logger.info(f"Getting almanac for start: {dayobs_start}, end: {dayobs_end}")
    almanac = Almanac(min_dayobs=dayobs_start, max_dayobs=dayobs_end)
    return almanac
