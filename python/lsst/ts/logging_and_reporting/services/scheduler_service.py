import logging
from datetime import datetime, timedelta

from rubin_sim.sim_archive import fetch_sim_stats_for_night

logger = logging.getLogger(__name__)


def get_expected_exposures(
    dayobs_start: int,
    dayobs_end: int,
) -> dict:
    """Retrieve the expected exposures for Simonyi for a specified range
    of observation nights.

    Parameters
    ----------
    dayobs_start : `int`
        The starting observation day (as an integer, e.g., YYYYMMDD).
    dayobs_end : `int`
        The ending observation day (as an integer, e.g., YYYYMMDD).

    Returns
    -------
    result : `dict`
        Result dictionary with key:
        ``"sum"``
            Sum of all expected exposures in the range (`int`).
    """

    logger.info(f"Getting expected exposures for dayobs_start: {dayobs_start}, dayobs_end: {dayobs_end}.")

    expected_exposures_list = []

    try:
        # Convert to datetime objects
        start_date = datetime.strptime(str(dayobs_start), "%Y%m%d")
        end_date = datetime.strptime(str(dayobs_end), "%Y%m%d")

        # Loop through range of dayobs
        current_date = start_date
        while current_date <= end_date:
            dayobs = int(current_date.strftime("%Y%m%d"))
            try:
                # Can only reach sims <60 days from current date
                expected_exposures = fetch_sim_stats_for_night(day_obs=dayobs, max_simulation_age=60)
                visits = expected_exposures.get("nominal_visits", 0)
                expected_exposures_list.append(visits)
                logger.info(f"dayobs {dayobs}: {visits} expected exposures")
            except Exception as e:
                logger.warning(f"Failed to fetch expected exposures for {dayobs}: {e}")
                raise

            current_date += timedelta(days=1)

        # Sum expected values together for one total over queried range
        sum_expected_exposures = sum(expected_exposures_list)
        logger.info(f"Sum of expected exposures in range: {sum_expected_exposures}")

        return {"sum": sum_expected_exposures}

    except Exception as e:
        logger.error(f"Error in getting expected exposures from rubin_sim: {e}", exc_info=True)
        raise
