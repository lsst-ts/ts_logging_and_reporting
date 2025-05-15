import datetime
from lsst.ts.logging_and_reporting.almanac import Almanac

def get_almanac(dayobs_start: datetime.date, dayobs_end: datetime.date) -> list:
    print(f"Getting almanac for start: {dayobs_start}, end: {dayobs_end}")
    almanac = Almanac(min_dayobs=dayobs_start, max_dayobs=dayobs_end)
    night_hours = almanac.night_hours
    return night_hours
