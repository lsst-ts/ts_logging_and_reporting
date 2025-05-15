import logging
from datetime import datetime
import numpy as np
from astropy.table import Table

from lsst.ts.logging_and_reporting.consdb import ConsdbAdapter
import lsst.ts.logging_and_reporting.utils as nd_utils

logger = logging.getLogger(__name__)


def convert_row(row):
    return {key: (row[key].item() if isinstance(row[key], np.generic) else row[key]) for key in row.keys()}


def get_mock_exposures(dayobs_start: datetime.date, dayobs_end: datetime.date, telescope: str) -> list:
    exposure_table = Table.read("data/exposures-lsstcam0413.ecsv")
    exposures = [convert_row(exp) for exp in exposure_table]
    return exposures


def get_exposures(dayobs_start: datetime.date, dayobs_end: datetime.date, telescope: str) :
    """
    Get exposures from the ConsDB for a given time range and telescope.
    """
    try:
        print(f"Getting exposures for start: "
              f"{dayobs_start}, end: {dayobs_end} "
              f"and telescope: {telescope}")
        cons_db = ConsdbAdapter(
            server_url=nd_utils.Server.get_url(),
            max_dayobs=nd_utils.datetime_to_dayobs(dayobs_end),
            min_dayobs=nd_utils.datetime_to_dayobs(dayobs_start),
        )
        print(f"max_dayobs: {cons_db.max_dayobs}, min_dayobs: {cons_db.min_dayobs}")
        exposures = cons_db.get_exposures(instrument=telescope)
        print(f"Exposures: {exposures}")
        return exposures

    except Exception as e:
        print(f"Error getting exposures: {e}")
        return {}


def custom_get_exposures(dayobs_start: datetime.date, dayobs_end: datetime.date, telescope: str) -> dict:
    try:
        exposures = {}
        logger.info(f"Getting exposures for start: {dayobs_start}, "
                    f"end: {dayobs_end} and telescope: {telescope}")
        cons_db = ConsdbAdapter(
            server_url=nd_utils.Server.get_url(),
            max_dayobs=nd_utils.datetime_to_dayobs(dayobs_end),
            min_dayobs=nd_utils.datetime_to_dayobs(dayobs_start),
        )
        logger.info(
            f"max_dayobs: {cons_db.max_dayobs}, "
            f"min_dayobs: {cons_db.min_dayobs}, "
            f"telescope: {telescope}"
        )
        ssql = f"""SELECT exposure_id, exp_time
          FROM cdb_{telescope}.exposure e, cdb_{telescope}.visit1_quicklook q
          WHERE e.exposure_id = q.visit_id
              AND {nd_utils.dayobs_int(cons_db.min_dayobs)} <= e.day_obs
              AND e.day_obs < {nd_utils.dayobs_int(cons_db.max_dayobs)}
        """

        sql = " ".join(ssql.split())
        exposures = cons_db.query(sql)
        if cons_db.verbose and len(exposures) > 0:
            logger.debug(f"Debug cdb.get_exposures {telescope=} {sql=}")
            logger.debug(f"Debug cdb.get_exposures: {exposures[0]=}")

    except Exception as e:
        print(f"Error getting exposures: {e}")
    return exposures
