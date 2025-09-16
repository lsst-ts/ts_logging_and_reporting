import logging
from astropy.time import Time, TimeDelta
import numpy as np

# from rubin_nights import connections
from rubin_nights import scriptqueue, connections
import rubin_nights.dayobs_utils as rn_dayobs

logger = logging.getLogger(__name__)

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

# TODO: error handling
def get_context_feed(
    dayobs_start: int,
    dayobs_end: int,
    auth_token: str = None
) -> list:
    """Get context feed data for a given time range."""
    logger.info(
        f"Getting context feed data for start: {dayobs_start}, end: {dayobs_end}"
    )
    # Update this to use connections.get_clients once PR merged
    endpoints = connections.get_clients(auth_token=auth_token)

    # Convert dayobs_start and dayobs_end to t_start and t_end
    t_start = Time(f"{rn_dayobs.day_obs_int_to_str(dayobs_start)}T12:00:00", format='isot', scale='utc')
    t_end = Time(
        f"{rn_dayobs.day_obs_int_to_str(dayobs_end)}T12:00:00",
        format="isot",
        scale="utc",
    ) + TimeDelta(1, format="jd")

    # Returns pandas dataframe and list
    df, cols = scriptqueue.get_consolidated_messages(t_start, t_end, endpoints)

    # For now, we drop all data not in columns=cols.
    # The dropped columns contain the raw data.

    # If we end up needing the raw data, we will need to handle
    # the non-serialisable types or bypass the default JSON
    # encoding and return a Response directly, e.g.
    # records = df.to_dict(orient="records") # list of row dicts
    # json_ready = jsonable_encoder(records) # fixes problem types
    # return JSONResponse(content=json_ready)

    # Discard all columns that are not listed in cols
    df_cols_only = df[df.columns.intersection(cols)]

    # Convert special floats (nans and infs) to strings
    # This ensures that JSON serialisation does not fail
    df_safe = df_cols_only.map(stringify_special_floats)
    records = df_safe.to_dict(orient="records")

    return [records, cols]
