import logging

import numpy as np
import pandas as pd
from astropy.table import Table

import lsst.ts.logging_and_reporting.utils as nd_utils
from lsst.ts.logging_and_reporting.consdb import ConsdbAdapter
from lsst.ts.logging_and_reporting.utils import stringify_special_floats

logger = logging.getLogger(__name__)


def convert_row(row):
    return {key: (row[key].item() if isinstance(row[key], np.generic) else row[key]) for key in row.keys()}


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
    logger.info(f"Getting exposures for start: {dayobs_start}, end: {dayobs_end} and telescope: {telescope}")
    cons_db = ConsdbAdapter(
        server_url=nd_utils.Server.get_url(),
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
        auth_token=auth_token,
    )
    logger.debug(
        f"max_dayobs: {cons_db.max_dayobs}, min_dayobs: {cons_db.min_dayobs}, telescope: {telescope}"
    )
    ssql = f"""
        SELECT e.exposure_id, e.exposure_name, e.exp_time, e.img_type,
              e.observation_reason, e.science_program, e.target_name,
              e.can_see_sky, e.band, e.obs_start, e.physical_filter,
              e.day_obs, e.seq_num, e.obs_end,
              e.exp_midpt_mjd, e.obs_start_mjd, e.obs_end_mjd,
              e.s_dec, e.s_ra, e.sky_rotation,
              q.zero_point_median, q.visit_id,
              q.pixel_scale_median, q.psf_sigma_median
        FROM cdb_{telescope}.exposure e, cdb_{telescope}.visit1_quicklook q
        WHERE e.exposure_id = q.visit_id
        AND {nd_utils.dayobs_int(cons_db.min_dayobs)} <= e.day_obs
        AND e.day_obs < {nd_utils.dayobs_int(cons_db.max_dayobs)}
        ORDER BY e.seq_num ASC
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
        f"max_dayobs: {cons_db.max_dayobs}, min_dayobs: {cons_db.min_dayobs}, telescope: {telescope}"
    )
    # Returns a pandas DataFrame
    data_log = cons_db.get_exposures(instrument=telescope)
    transformed_efd_data = cons_db.get_transformed_efd_data(instrument=telescope)
    if len(data_log) > 0 and len(transformed_efd_data) > 0:
        # Add transformed efd dataframe to data_log dataframe
        data_log = pd.merge(data_log, transformed_efd_data, on="exposure id", how="inner")

    # Convert special floats (nans and infs) to strings
    # This ensures that JSON serialisation does not fail
    df_safe = data_log.map(stringify_special_floats)
    records = df_safe.to_dict(orient="records")

    if cons_db.verbose and len(data_log) > 0:
        logger.debug(f"Debug cdb.get_data_log {telescope=} {dayobs_start=} {dayobs_end=}")
        logger.debug(f"Debug cdb.get_data_log: {data_log[0]=}")

    return records
