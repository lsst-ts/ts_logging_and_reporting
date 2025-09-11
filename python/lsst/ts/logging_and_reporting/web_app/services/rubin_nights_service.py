import logging
import pandas as pd
import numpy as np
from astropy.time import Time

import rubin_nights.dayobs_utils as rn_dayobs
import rubin_nights.rubin_scheduler_addons as rn_sch
import rubin_nights.augment_visits as rn_aug
from rubin_nights.observatory_status import get_dome_open_close
from rubin_nights.influx_query import InfluxQueryClient


logger = logging.getLogger(__name__)

WAIT_BEFORE_SLEW = 1.45
SETTLE = 2.0

def make_json_safe(obj):
    """
    Recursively converts objects to be JSON serializable.

    This function traverses the input object, converting
    any non-JSON-serializable types (such as NumPy integers,
    floats, NaN, or infinity) into types that can be safely serialized.
    Dictionaries and lists are processed recursively.
    NumPy integer and floating types are converted to their native Python
    counterparts. NaN and infinity values are replaced with None.

    Parameters
    ----------
    obj : any
        The object to convert. Can be a dict, list, or any value.

    Returns
    -------
    any
        The converted object, safe for JSON serialization.
    """
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    else:
        return obj


def get_time_accounting(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    exposures: list,
    efd_client: InfluxQueryClient,
) -> pd.DataFrame:
    """
    Retrieve and process time accounting data for a given instrument
    and range of observation days.
    This function takes a list of exposures and augments them with
    visit information, calculates model slew times, and computes
    valid overheads for each visit.

    Parameters
    ----------
    dayObsStart : int
        The starting dayObs (observation day) for which to retrieve data.
    dayObsEnd : int
        The ending dayObs (observation day) for which to retrieve data.
    instrument : str
        The name of the instrument for which to retrieve time accounting data.
    exposures : list
        A list of exposure dictionaries or objects to be processed.
    efd_client : InfluxQueryClient
        An EFD client instance used for retrieving additional data.
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed visit
        and overhead information.
        Returns an empty DataFrame if no exposures
        are provided or if an error occurs.
    """
    logger.info(
        f"Getting time accounting data for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        if len(exposures) == 0:
            return pd.DataFrame()

        exposures_df = pd.DataFrame(exposures)
        visits = rn_aug.augment_visits(exposures_df, "lsstcam", skip_rs_columns=True)

        visits, _ = rn_sch.add_model_slew_times(
            visits, efd_client, model_settle=WAIT_BEFORE_SLEW + SETTLE, dome_crawl=False)
        max_scatter = 6
        valid_overhead = np.min([np.where(np.isnan(visits.slew_model.values), 0, visits.slew_model.values)
                                    + max_scatter, visits.visit_gap.values], axis=0)
        visits['overhead'] = valid_overhead

        visits = visits.replace([np.inf, -np.inf], np.nan)
        visits = visits.where(pd.notnull(visits), None)

        return visits

    except Exception as e:
        logger.error(
            f"Error in getting time accounting data from rubin_nights through EFD: {e}",
            exc_info=True)
        return pd.DataFrame()


def get_open_close_dome(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    efd_client: InfluxQueryClient,
 ) -> dict:
    """
    Retrieve the dome open and close times for a specified
    range of observation days and instrument.
    Currently only for Simonyi.

    Parameters
    ----------
    dayObsStart : int
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayObsEnd : int
        The ending observation day (as an integer, e.g., YYYYMMDD).
    instrument : str
        The name of the instrument for which to retrieve dome open/close times.
    efd_client : InfluxQueryClient
        An EFD client instance to use for data retrieval.
        If None, a default client may be used.
    Returns
    -------
    dict
        A dictionary containing the dome open and close times.
    """
    logger.info(
        f"Getting open/close dome times from rubin-nights for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        day_min = Time(f"{rn_dayobs.day_obs_int_to_str(dayObsStart)}T12:00:00", format='isot', scale='utc')
        day_max = Time(f"{rn_dayobs.day_obs_int_to_str(dayObsEnd)}T12:00:00", format='isot', scale='utc')
        dome_open = get_dome_open_close(day_min, day_max, efd_client)
        return dome_open
    except Exception as e:
        logger.error(f"Error getting open/close dome times from rubin_nights through EFD: {e}", exc_info=True)
        return pd.DataFrame()
