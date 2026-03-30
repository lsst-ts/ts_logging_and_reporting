import logging

from lsst.ts.logging_and_reporting.jira import JiraAdapter

logger = logging.getLogger(__name__)


INSTRUMENTS = {
    "LATISS": "AuxTel",
    "LSSTCam": "Simonyi",
}

INSTRUMENT_EXCLUDE_MAP = {
    "LSSTCam": ["LATISS"],
}


def filter_tickets_with_instrument_match(tickets, instrument):
    """Filters a list of JIRA tickets to **include** only those
    associated with a specified instrument.
    For each ticket, checks if any of the system names in the ticket's
    'system' field contains (string includes) the instrument name
    or its corresponding value from the INSTRUMENTS dictionary.
    If a match is found, adds a 'url' field to the
    ticket containing a direct link to the JIRA ticket.

    Parameters
    ----------
    tickets : `list[dict]`
        List of JIRA ticket dictionaries,
        each containing at least 'system' and 'key' fields.
    instrument : `str`
        The instrument name to filter (include) tickets by.

    Returns
    -------
    filtered_tickets : `list[dict]`
        Filtered list of tickets that match the instrument,
        each with an added 'url' field.
    """

    def matches(ticket):
        # Get the list of systems from the object
        obj_system_list = ticket["system"]
        search_terms = (instrument, INSTRUMENTS[instrument])
        # Check if any search term appears in any system name
        matched = any(term in system for term in search_terms for system in obj_system_list)
        return matched

    return [ticket for ticket in tickets if matches(ticket)]


def filter_tickets_without_instrument_match(tickets, instrument):
    """Filters a list of JIRA tickets to **exclude** those
    associated with a specified instrument.
    For each ticket, checks if none of the system names in the ticket's
    'system' field contains (string includes) the instrument name
    or its corresponding value from the INSTRUMENTS dictionary.
    If no match is found, adds a 'url' field to the
    ticket containing a direct link to the JIRA ticket.

    Parameters
    ----------
    tickets : `list[dict]`
        List of JIRA ticket dictionaries,
        each containing at least 'system' and 'key' fields.
    instrument : `str`
        The instrument name to filter (exclude) tickets by.

    Returns
    -------
    filtered_tickets : `list[dict]`
        Filtered list of tickets that do not match the instrument,
        each with an added 'url' field.
    """

    def not_matches(ticket):
        # Get the list of systems from the object
        obj_system_list = ticket["system"]
        search_terms = (instrument, INSTRUMENTS[instrument])
        # Check if any search term appears in any system name
        matched = any(term in system for term in search_terms for system in obj_system_list)
        return not matched

    return [ticket for ticket in tickets if not_matches(ticket)]


def get_jira_tickets(
    dayobs_start: int,
    dayobs_end: int,
    telescope: str,
    jira_token: str = None,
    jira_hostname: str = None,
) -> list[dict]:
    """Retrieve and filter Jira tickets for a given dayobs range and telescope.

    This service queries Jira for observation-related tickets within the
    specified dayobs range, then applies instrument-based filtering logic
    to return only relevant tickets for the requested telescope.

    Parameters
    ----------
    dayobs_start : `int`
        The start of the dayobs range (inclusive), formatted as YYYYMMDD.
    dayobs_end : `int`
        The end of the dayobs range (exclusive), formatted as YYYYMMDD.
    telescope : `str`
        The telescope or instrument name used to filter returned tickets.
    jira_token : `str`, optional
        Authentication token for Jira API access.
    jira_hostname : `str`, optional
        Hostname for the Jira API.

    Returns
    -------
    list of `dict`
        A list of Jira ticket objects matching the specified criteria.
        Returns an empty list if no tickets are found.

    Notes
    -----
    - Tickets are initially retrieved using the Jira adapter over the
      full dayobs range.
    - If ``telescope`` is present in ``INSTRUMENT_EXCLUDE_MAP``, tickets
      are filtered by excluding those matching specified instruments.
    - Otherwise, only tickets matching the given ``telescope`` are included.
    - Filtering is performed using helper functions that match or exclude
      tickets based on instrument/system fields.

    Raises
    ------
    None
        This function does not raise exceptions directly, but may propagate
        exceptions raised by the Jira adapter or underlying network calls.
    """
    logger.info(f"Jira service: start: {dayobs_start}, end: {dayobs_end} and telescope: {telescope}")

    jira = JiraAdapter(
        jira_token=jira_token,
        jira_hostname=jira_hostname,
    )

    tickets = jira.get_obs_issues(
        min_dayobs=dayobs_start,
        max_dayobs=dayobs_end,
    )
    if not tickets:
        logger.warning("No Jira tickets found for the specified date range and telescope.")
        return []
    logger.info(f"Found {len(tickets)} Jira tickets.")

    # If telescope is in INSTRUMENT_EXCLUDE_MAP,
    # show all tickets except those with systems in the exclude list
    # else show only tickets matching the telescope.
    if telescope in INSTRUMENT_EXCLUDE_MAP:
        system_tickets = tickets
        for excluded_instrument in INSTRUMENT_EXCLUDE_MAP[telescope]:
            system_tickets = filter_tickets_without_instrument_match(
                system_tickets,
                instrument=excluded_instrument,
            )
    else:
        system_tickets = filter_tickets_with_instrument_match(
            tickets,
            instrument=telescope,
        )

    return system_tickets


def get_block_ticket_summaries(
    ticket_keys: list[str],
    jira_token: str = None,
    jira_hostname: str = None,
) -> dict:
    """
    Fetch summaries for a list of BLOCK Jira tickets.

    Parameters
    ----------
    ticket_keys : `list[str]`
        List of Jira issue keys (e.g. ["BLOCK-123", "BLOCK-456"])
    jira_token : `str`, optional
        Authentication token used when connecting to Rubin Observatory's
        Jira services.
    jira_hostname : `str`, optional
        Hostname for the Jira API.

    Returns
    -------
    dict
        Mapping of ticket key -> summary
    """
    logger.info(f"Jira service (BLOCK): fetching summaries for {len(ticket_keys)} tickets")

    if not ticket_keys:
        logger.warning("No BLOCK ticket keys provided.")
        return {}

    jira = JiraAdapter(
        jira_token=jira_token,
        jira_hostname=jira_hostname,
    )

    summaries = jira.fetch_block_ticket_summaries(ticket_keys)

    if not summaries:
        logger.warning("No BLOCK ticket summaries found.")
    else:
        logger.info(f"Fetched {len(summaries)} BLOCK ticket summaries.")

    return summaries
