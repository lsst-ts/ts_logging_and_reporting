import logging
from lsst.ts.logging_and_reporting.source_adapters import ExposurelogAdapter

logger = logging.getLogger(__name__)


def get_exposure_flags(
    min_dayobs: str,
    max_dayobs: str,
    instrument: str,
    verbose: bool = False,
    limit: int = 2500,
) -> list[dict]:
    """
    Get all records with non-empty exposure_flag from the
    Exposure Log for a specific instrument.

    Parameters
    ----------
    min_dayobs : str
        Inclusive lower bound for day_obs (e.g., "2025-06-01").
    max_dayobs : str
        Exclusive upper bound for day_obs (e.g., "2025-06-03").
    instrument : str
        Instrument to filter by (e.g., "LSSTComCam").
    verbose : bool
        Enable verbose logging/debugging.
    limit : int
        Maximum number of records to request per page (default 2500).

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
    )

    logger.info(f"Fetching exposure flags for instrument: {instrument}")

    status = adapter.status.get("messages")
    logger.debug(f"ExposureLogAdapter status: {status}")

    if status is None or status.get("error") is not None:
        raise Exception(
            f"Error getting exposure log messages from {status.get('endpoint_url')}: {status.get('error')}"
        )

    records = adapter.messages.get(instrument, [])
    if not records:
        verbose and logger.debug("No messages for this instrument.")
        return []

    flags = {"questionable", "junk"}
    flagged = [
        {"obs_id": rec["obs_id"], "exposure_flag": rec["exposure_flag"]}
        for rec in records
        if rec.get("exposure_flag") and rec["exposure_flag"] in flags
    ]

    if verbose:
        logger.debug(f"Retrieved {len(flagged)} flagged records")

    return flagged
