import logging
import traceback
from datetime import datetime, timedelta

from lsst.ts.logging_and_reporting.almanac import Almanac

logger = logging.getLogger(__name__)


def get_almanac_night_hours(dayobs_start: int, dayobs_end: int) -> list:
    logger.info(f"Getting almanac for start: {dayobs_start}, end: {dayobs_end}")
    try:
        start = datetime.strptime(str(dayobs_start), '%Y%m%d')
        end = datetime.strptime(str(dayobs_end), '%Y%m%d')

        # calculate night hours for each day in the range
        current = start
        total_night_hours = 0.0
        while current < end:
            dayobs = int(current.strftime('%Y%m%d'))
            almanac = Almanac(min_dayobs=dayobs_start, max_dayobs=dayobs)
            total_night_hours += almanac.night_hours
            current += timedelta(days=1)
        return total_night_hours
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error fetching almanac data for: {dayobs_start}, {dayobs_end}. Error: {e}")
        raise e
