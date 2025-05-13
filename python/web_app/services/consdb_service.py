from datetime import datetime
import numpy as np
from astropy.table import Table

from lsst.ts.logging_and_reporting.consdb import ConsdbAdapter

def convert_row(row):
    return {key: (row[key].item() if isinstance(row[key], np.generic) else row[key]) for key in row.keys()}


def get_exposures(dayobs_start: datetime.date, dayobs_end: datetime.date, telescope: str) -> list:
    print(f"Getting exposures for start: {dayobs_start}, end: {dayobs_end} and telescope: {telescope}")
    # TODO: replace with code to retrieve exposure information using data adaptors
    exposure_table = Table.read("data/exposures-lsstcam0413.ecsv")
    exposures = [convert_row(exp) for exp in exposure_table]
    return exposures


def get_exposures_from_adapter(dayobs_start: datetime.date, dayobs_end: datetime.date, telescope: str) -> list:
    print(f"Getting exposures for start: {dayobs_start}, end: {dayobs_end} and telescope: {telescope}")
    consdbAdapter = ConsdbAdapter(
        telescope=telescope,
        min_dayobs=dayobs_start,
        max_dayobs=dayobs_end,
        # instrument="LSSTCam",
        # registry=1
    )