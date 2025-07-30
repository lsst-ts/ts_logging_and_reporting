import logging
import numpy as np
from astropy.table import Table

from lsst.ts.logging_and_reporting.consdb import ConsdbAdapter
import lsst.ts.logging_and_reporting.utils as nd_utils


logger = logging.getLogger(__name__)


def convert_row(row):
    return {
        key: (row[key].item() if isinstance(row[key], np.generic) else row[key])
        for key in row.keys()
    }


# Required to jasonify pandas DataFrame with special float values
# To ensure JSON serialisation does not fail
def stringify_special_floats(val):
    if isinstance(val, float):
        if np.isnan(val):
            return "NaN"
        elif np.isposinf(val):
            return "Infinity"
        elif np.isneginf(val):
            return "-Infinity"
    return val


def get_mock_exposures(dayobs_start: int, dayobs_end: int, telescope: str) -> list:
    exposure_table = Table.read("data/exposures-lsstcam0413.ecsv")
    exposures = [convert_row(exp) for exp in exposure_table]
    return exposures


def get_exposures(
    dayobs_start: int,
    dayobs_end: int,
    telescope: str,
    auth_token: str = None,
) -> dict:

    exposures = {}
    logger.info(
        f"Getting exposures for start: {dayobs_start}, "
        f"end: {dayobs_end} and telescope: {telescope}"
    )
    cons_db = ConsdbAdapter(
        server_url=nd_utils.Server.get_url(),
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
        auth_token=auth_token,
    )
    logger.debug(
        f"max_dayobs: {cons_db.max_dayobs}, "
        f"min_dayobs: {cons_db.min_dayobs}, "
        f"telescope: {telescope}"
    )
    ssql = f"""
        SELECT e.exposure_id, e.exposure_name, e.exp_time, e.img_type,
              e.observation_reason, e.science_program, e.target_name,
              e.can_see_sky, e.band, e.dimm_seeing, e.obs_start,
              e.physical_filter, e.day_obs,
              q.zero_point_median, q.visit_id
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
    return exposures


def get_data_log(
    dayobs_start: int,
    dayobs_end: int,
    telescope: str,
    auth_token: str = None,
) -> dict:
    """
    Get Data Log fields from the ConsDB for a given time range and telescope.
    """

    data_log = {}
    logger.info(
        f"""
        Getting data log for start:
        {dayobs_start}, end: {dayobs_end}
        and telescope: {telescope}"""
    )
    cons_db = ConsdbAdapter(
        server_url=nd_utils.Server.get_url(),
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
        auth_token=auth_token,
    )
    logger.debug(
        f"max_dayobs: {cons_db.max_dayobs}, "
        f"min_dayobs: {cons_db.min_dayobs}, "
        f"telescope: {telescope}"
    )
    # Returns a pandas DataFrame
    data_log = cons_db.get_exposures(instrument=telescope)

    # Convert special floats (nans and infs) to strings
    # This ensures that JSON serialisation does not fail
    df_safe = data_log.map(stringify_special_floats)
    records = df_safe.to_dict(orient="records")

    if cons_db.verbose and len(data_log) > 0:
        logger.debug(
            f"Debug cdb.get_data_log {telescope=} {dayobs_start=} {dayobs_end=}"
        )
        logger.debug(f"Debug cdb.get_data_log: {data_log[0]=}")

    return records


def get_transformed_efd(
    dayobs_start: int,
    dayobs_end: int,
    telescope: str,
    auth_token: str = None,
) -> dict:

    exposures = {}
    logger.info(
        f"Getting transformed efd data for start: {dayobs_start}, "
        f"end: {dayobs_end} and telescope: {telescope}"
    )

    cons_db = ConsdbAdapter(
        server_url=nd_utils.Server.get_url(),
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
        auth_token=auth_token,
    )

    logger.debug(
        f"max_dayobs: {cons_db.max_dayobs}, "
        f"min_dayobs: {cons_db.min_dayobs}, "
        f"telescope: {telescope}"
    )
    # TODO: These temperatures return None, follow up with OSW-779
    temperatures = [
        "mt_salindex102_temperature_0_mean",
        "mt_salindex102_temperature_0_stddev",
        "mt_salindex102_temperature_0_min",
        "mt_salindex102_temperature_0_max",
    ]

    ssql = f"""
        SELECT
            exposure_id,
            created_at,
            {", ".join(temperatures)}
        FROM
            efd_{telescope}.exposure_efd e
        WHERE
            exposure_id
            BETWEEN {nd_utils.dayobs_int(cons_db.min_dayobs)}00000
            AND {nd_utils.dayobs_int(cons_db.max_dayobs)}99999;
        """

    sql = " ".join(ssql.split())
    exposures = cons_db.query(sql)

    if cons_db.verbose and len(exposures) > 0:
        logger.debug(f"Debug cdb.get_exposures {telescope=} {sql=}")
        logger.debug(f"Debug cdb.get_exposures: {exposures[0]=}")

    return exposures
