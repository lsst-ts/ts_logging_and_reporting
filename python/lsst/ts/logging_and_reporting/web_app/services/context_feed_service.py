import logging
import os
from astropy.time import Time, TimeDelta
import numpy as np

# from rubin_nights import connections
from rubin_nights import scriptqueue
import rubin_nights.dayobs_utils as rn_dayobs

from rubin_nights.consdb_query import ConsDbFastAPI, ConsDbTap
from rubin_nights.influx_query import InfluxQueryClient
from rubin_nights.logging_query import ExposureLogClient, NarrativeLogClient, NightReportClient

logger = logging.getLogger(__name__)

# Modified version of connections.get_clients to allow passing of auth_token
# Remove and use connections.get_clients once PR merged
def get_clients(auth_token: str | None = None, site: str | None = None) -> dict:

    auth = ("user", auth_token)

    api_endpoints = {
        "usdf": "https://usdf-rsp.slac.stanford.edu",
        "usdf-dev": "https://usdf-rsp-dev.slac.stanford.edu",
        "summit": "https://summit-lsp.lsst.codes",
    }

    if site is None:
        # Guess site from EXTERNAL_INSTANCE_URL (set for RSPs)
        location = os.getenv("EXTERNAL_INSTANCE_URL", "")
        if "summit-lsp" in location:
            site = "summit"
        elif "usdf-rsp-dev" in location:
            site = "usdf-dev"
        elif "usdf-rsp" in location:
            site = "usdf"
        # Otherwise, use the USDF resources, outside of the RSP
        if site is None:
            site = "usdf"
    else:
        site = site

    api_base = api_endpoints[site]
    narrative_log = NarrativeLogClient(api_base, auth)
    exposure_log = ExposureLogClient(api_base, auth)
    night_report = NightReportClient(api_base, auth)
    consdb_query = ConsDbFastAPI(api_base, auth)
    consdb_tap = ConsDbTap(api_base, token=auth_token)
    efd_client = InfluxQueryClient(site, db_name="efd")
    obsenv_client = InfluxQueryClient(site, db_name="lsst.obsenv")
    sasquatch_client = InfluxQueryClient("usdfdev", db_name="lsst.dm")

    # Be extra helpful with environment variables if using USDF for LFA
    if "usdf" in site:
        # And some env variables for S3 through USDF
        os.environ["LSST_DISABLE_BUCKET_VALIDATION"] = "1"
        os.environ["S3_ENDPOINT_URL"] = "https://s3dfrgw.slac.stanford.edu/"
    # Or if you're actually using one of the USDF RSPs (or kubernetes)
    if "usdf" in os.getenv("EXTERNAL_INSTANCE_URL", ""):
        if os.getenv("RUBIN_SIM_DATA_DIR") is None:
            # Use shared RUBIN_SIM_DATA_DIR
            os.environ["RUBIN_SIM_DATA_DIR"] = "/sdf/data/rubin/shared/rubin_sim_data"

    endpoints = {
        "api_base": api_base,
        "efd": efd_client,
        "obsenv": obsenv_client,
        "sasquatch": sasquatch_client,
        "consdb": consdb_query,
        "consdb_tap": consdb_tap,
        "narrative_log": narrative_log,
        "exposure_log": exposure_log,
        "night_report": night_report,
    }
    logger.info(f"Endpoint base url: {endpoints['api_base']}")

    return endpoints

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
    # endpoints = connections.get_clients(tokenfile=tokenfile, site=site)
    endpoints = get_clients(auth_token=auth_token)

    # Convert dayobs_start and dayobs_end to t_start and t_end
    t_start = Time(f"{rn_dayobs.day_obs_int_to_str(dayobs_start)}T12:00:00", format='isot', scale='utc')
    t_end = Time(
                 f"{rn_dayobs.day_obs_int_to_str(dayobs_end)}T12:00:00", format='isot', scale='utc'
                ) + TimeDelta(1, format='jd')

    # Returns pandas dataframe and list
    df, cols = scriptqueue.get_consolidated_messages(t_start, t_end, endpoints)

    # Discard all columns that are not listed in cols
    df_cols_only = df[df.columns.intersection(cols)]

    # Convert special floats (nans and infs) to strings
    # This ensures that JSON serialisation does not fail
    df_safe = df_cols_only.map(stringify_special_floats)
    records = df_safe.to_dict(orient="records")

    return [records, cols]
