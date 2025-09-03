import logging
import os

from rubin_nights.consdb_query import ConsDbFastAPI
from rubin_nights.influx_query import InfluxQueryClient

logger = logging.getLogger(__name__)

# Copied from Context Feed work currently in progress
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
    consdb_query = ConsDbFastAPI(api_base, auth)
    efd_client = InfluxQueryClient(site, db_name="efd")
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
        "consdb": consdb_query,
    }
    logger.info(f"Endpoint base url: {endpoints['api_base']}")

    return endpoints
