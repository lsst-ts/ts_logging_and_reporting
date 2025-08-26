import logging
import os
from astropy.time import Time, TimeDelta
import astropy.units as u
import pandas as pd
import numpy as np

from rubin_nights import connections
from rubin_nights import scriptqueue
from rubin_nights import scriptqueue_formatting
import rubin_nights.dayobs_utils as rn_dayobs

from rubin_nights.consdb_query import ConsDbFastAPI, ConsDbTap
from rubin_nights.influx_query import InfluxQueryClient
from rubin_nights.logging_query import ExposureLogClient, NarrativeLogClient, NightReportClient

logger = logging.getLogger(__name__)


# def get_night_reports(
#     dayobs_start: int,
#     dayobs_end: int,
#     auth_token: str = None
# ) -> list:
#     """Get nightreport records for a given time range."""
#     logger.info(
#         f"Getting night reports for start: {dayobs_start}, end: {dayobs_end}"
#     )
#     nightreport = NightReportAdapter(
#         server_url=nd_utils.Server.get_url(),
#         max_dayobs=dayobs_end,
#         min_dayobs=dayobs_start,
#         auth_token=auth_token,
#     )
#     status = nightreport.get_records()
#     logger.debug(f"status: {status}")
#     if status.get('error') is not None:
#         raise Exception(
#             f"Error getting nightreport records from"
#             f" {status.endpoint_url}: {status.error}"
#         )
#     return nightreport.records


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


def get_context_feed(
    dayobs_start: int,
    dayobs_end: int,
    auth_token: str = None
) -> list:
    """Get context feed data for a given time range."""
    logger.info(
        f"Getting context feed data for start: {dayobs_start}, end: {dayobs_end}"
    )

    print(f"CONTEXT FEED service begins.")

    # Can't use get_clients as is, since auth token is expected to be in a file
    # endpoints = connections.get_clients(tokenfile=tokenfile, site=site)
    endpoints = get_clients(auth_token=auth_token)

    # print(f"CONTEXT FEED endpoints: ", endpoints)

    # Convert dayobs_start and dayobs_end to t_start and t_end
    t_start = Time(f"{rn_dayobs.day_obs_int_to_str(dayobs_start)}T12:00:00", format='isot', scale='utc')
    t_end = Time(f"{rn_dayobs.day_obs_int_to_str(dayobs_end)}T12:00:00", format='isot', scale='utc') + TimeDelta(1, format='jd')

    df, cols = scriptqueue.get_consolidated_messages(t_start, t_end, endpoints)
    print(f"TYPE df: ", type(df))
    print(f"CONTEXT FEED cols: ", cols)
    print(f"CONTEXT FEED df: ", df)

    # Convert special floats (nans and infs) to strings
    # This ensures that JSON serialisation does not fail
    df_safe = df.map(stringify_special_floats)
    records = df_safe.to_dict(orient="records") # list!
    print(f"CONTEXT FEED conversion to dict completed.")

    # print(f"CONTEXT FEED df_to_dict: ", records)
    print(f"TYPE df_to_dict: ", type(records))

    print(f"CONTEXT FEED service completed.")

    return [records, cols]
