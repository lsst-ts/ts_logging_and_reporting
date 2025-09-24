import logging
import pandas as pd
import numpy as np
from astropy.time import Time, TimeDelta

import rubin_nights.dayobs_utils as rn_dayobs
import rubin_nights.rubin_scheduler_addons as rn_sch
import rubin_nights.augment_visits as rn_aug
from rubin_nights.observatory_status import get_dome_open_close
from rubin_nights.connections import get_clients
from rubin_nights.scriptqueue import get_consolidated_messages

from lsst.ts.logging_and_reporting.utils import stringify_special_floats


logger = logging.getLogger(__name__)

WAIT_BEFORE_SLEW = 1.45
SETTLE = 2.0


def get_time_accounting(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    exposures: list,
    auth_token: str = None,
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
    auth_token : str, optional
        Authentication token used when connecting to Rubin Observatory
        services.

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

        # Get connections to rubin_nights services
        clients = get_clients(auth_token=auth_token)

        exposures_df = pd.DataFrame(exposures)
        visits = rn_aug.augment_visits(exposures_df, "lsstcam", skip_rs_columns=True)

        visits, _ = rn_sch.add_model_slew_times(
            visits, clients["efd"], model_settle=WAIT_BEFORE_SLEW + SETTLE, dome_crawl=False)
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
    auth_token: str = None,
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
    auth_token : str, optional
        Authentication token used when connecting to Rubin Observatory
        services.

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
        # Get connections to rubin_nights services
        clients = get_clients(auth_token=auth_token)

        day_min = Time(f"{rn_dayobs.day_obs_int_to_str(dayObsStart)}T12:00:00", format='isot', scale='utc')
        day_max = Time(f"{rn_dayobs.day_obs_int_to_str(dayObsEnd)}T12:00:00", format='isot', scale='utc')
        dome_open = get_dome_open_close(day_min, day_max, clients["efd"])
        return dome_open
    except Exception as e:
        logger.error(f"Error getting open/close dome times from rubin_nights through EFD: {e}", exc_info=True)
        return pd.DataFrame()


def get_context_feed(
    dayobs_start: int,
    dayobs_end: int,
    auth_token: str = None,
) -> list:
    """
    Retrieve consolidated context feed messages for a given range of
    observation days.

    This function queries the Rubin Observatory services via the
    consolidated ScriptQueue messages, within the specified dayObs
    range. It processes the results into JSON-safe records, ensuring
    special float values (NaN, Infinity) are handled correctly for
    serialization. Only the relevant context feed columns are retained
    from the raw data. Any raw data outside these columns is dropped;
    if raw data is needed in the future, non-serialisable types must
    be handled explicitly (e.g., using jsonable_encoder).

    Parameters
    ----------
    dayobs_start : int
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayobs_end : int
        The ending observation day (as an integer, e.g., YYYYMMDD).
    auth_token : str, optional
        Authentication token used when connecting to external
        services.

    Returns
    -------
    list
        A list containing two elements:
        - records : list of dict
            JSON-safe context feed records, one per row.
        - cols : list of str
            The column names included in the context feed.
        Returns an empty list if an error occurs.
    """
    logger.info(
        f"Getting context feed data for start: {dayobs_start}, end: {dayobs_end}"
    )
    try:
        # Get connections to rubin_nights services
        endpoints = get_clients(auth_token=auth_token)

        # Convert dayobs_start and dayobs_end to t_start and t_end
        t_start = Time(f"{rn_dayobs.day_obs_int_to_str(dayobs_start)}T12:00:00", format='isot', scale='utc')
        t_end = Time(
            f"{rn_dayobs.day_obs_int_to_str(dayobs_end)}T12:00:00",
            format="isot",
            scale="utc",
        ) + TimeDelta(1, format="jd")

        # Returns pandas dataframe and list
        df, cols = get_consolidated_messages(t_start, t_end, endpoints)

        # Discard all columns that are not listed in cols
        df_cols_only = df[df.columns.intersection(cols)]

        # Convert special floats (nans and infs) to strings
        # This ensures that JSON serialisation does not fail
        df_safe = df_cols_only.map(stringify_special_floats)
        records = df_safe.to_dict(orient="records")

        return [records, cols]

    except Exception as e:
        logger.error(
            f"Error retrieving Context Feed data from rubin_nights: {e}",
            exc_info=True)
        return []
