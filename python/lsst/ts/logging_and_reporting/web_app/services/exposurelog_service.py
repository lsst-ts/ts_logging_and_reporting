import logging

from lsst.ts.logging_and_reporting.exposure_log import ExposurelogAdapter

logger = logging.getLogger(__name__)


def get_exposure_flags(
    min_dayobs: str,
    max_dayobs: str,
    instrument: str,
    verbose: bool = False,
    limit: int = 2500,
    auth_token: str = None,
) -> list[dict]:
    """
    Get all records with non-empty exposure_flag from the
    Exposure Log for a specific instrument.

    Parameters
    ----------
    min_dayobs : `str`
        Inclusive lower bound for day_obs (e.g., "2025-06-01").
    max_dayobs : `str`
        Exclusive upper bound for day_obs (e.g., "2025-06-03").
    instrument : `str`
        Instrument to filter by (e.g., "LSSTComCam").
    verbose : `bool`
        Enable verbose logging/debugging.
    limit : `int`
        Maximum number of records to request per page (default 2500).
    auth_token: `str`
        Authorization token to be passed to the ExposurelogAdapter.

    Returns
    -------
    List[dict]
        List of dicts with keys: 'obs_id' and 'exposure_flag'
    """

    adapter = ExposurelogAdapter(
        min_dayobs=min_dayobs,
        max_dayobs=max_dayobs,
        limit=limit,
        verbose=verbose,
        auth_token=auth_token,
    )

    logger.info(f"Fetching exposure flags for instrument: {instrument}")
    messages = adapter.get_messages(instrument=instrument)

    if not messages:
        verbose and logger.debug("No messages for this instrument.")
        return []

    flags = {"questionable", "junk"}
    flagged = [
        {"obs_id": entry["obs_id"], "exposure_flag": entry["exposure_flag"]}
        for entry in messages
        if entry.get("exposure_flag") and entry["exposure_flag"] in flags
    ]

    if verbose:
        logger.debug(f"Retrieved {len(flagged)} flagged records")

    return flagged


def get_exposurelog_entries(
    min_dayobs: str,
    max_dayobs: str,
    instrument: str,
    verbose: bool = False,
    limit: int = 2500,
    auth_token: str = None,
) -> list[dict]:
    """
    Fetch all Exposure Log entries for an instrument and dayobs range.

    Parameters
    ----------
    min_dayobs : `str`
        Inclusive lower bound for day_obs (e.g., "2025-06-01").
    max_dayobs : `str`
        Exclusive upper bound for day_obs (e.g., "2025-06-03").
    instrument : `str`
        Instrument to filter by (e.g., "LSSTComCam").
    verbose : `bool`
        Enable verbose logging/debugging.
    limit : `int`
        Maximum number of records to request per page (default 2500).
    auth_token: `str`
        Authorization token to be passed to the ExposurelogAdapter.

    Returns
    -------
    List[dict]
        List of each Exposure Log entry, each a dict.
    """
    adapter = ExposurelogAdapter(
        min_dayobs=min_dayobs,
        max_dayobs=max_dayobs,
        limit=limit,
        verbose=verbose,
        auth_token=auth_token,
    )

    # Get records
    # Are we paralellizing this from the front end? call this in pools?
    messages = adapter.get_messages(instrument=instrument)

    if verbose:
        logger.debug(f"Fetched {len(messages)} Exposure Log records for {instrument}")

    return messages
