import logging
import traceback
from datetime import datetime, timedelta

from lsst.ts.logging_and_reporting.almanac import Almanac

logger = logging.getLogger(__name__)


def get_almanac(dayobs_start: int, dayobs_end: int) -> list:
    logger.info(f"Getting almanac for start: {dayobs_start}, end: {dayobs_end}")
    try:
        start = datetime.strptime(str(dayobs_start), '%Y%m%d') + timedelta(days=1)
        end = datetime.strptime(str(dayobs_end), '%Y%m%d') + timedelta(days=1)
        almanac_info = []
        current = start
        while current < end:
            dayobs = int(current.strftime('%Y%m%d'))
            almanac = Almanac(min_dayobs=dayobs_start, max_dayobs=dayobs)
            night_events = almanac.as_dict[0]
            almanac_info.append({
                'dayobs': dayobs,
                'night_hours': almanac.night_hours,
                'twilight_evening': night_events['Evening Astronomical Twilight'],
                'twilight_morning': night_events['Morning Astronomical Twilight'],
                'moon_rise_time': night_events['Moon Rise'],
                'moon_set_time': night_events['Moon Set'],
                'moon_illumination': night_events['Moon Illumination'],
            })
            current += timedelta(days=1)
        return almanac_info
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error fetching almanac data for: {dayobs_start}, {dayobs_end}. Error: {e}")
        raise e
