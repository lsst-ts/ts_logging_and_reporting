import logging

from lsst.ts.logging_and_reporting.jira import JiraAdapter


logger = logging.getLogger(__name__)


INSTRUMENTS = {
    "LATISS": "AuxTel",
    "LSSTCam" : "Simonyi",
}


def filter_tickets_by_instrument(tickets, instrument):
    """Filters a list of JIRA tickets to include only those associated with a
    specified instrument.
    For each ticket, checks if any of the system names in the ticket's
    'system' field match the instrument name or its corresponding value from
    the INSTRUMENTS dictionary. If a match is found, adds a 'url' field to the
    ticket containing a direct link to the JIRA ticket.
    Parameters
    ----------
    tickets : `list[dict]`
        List of JIRA ticket dictionaries,
        each containing at least 'system' and 'key' fields.
    instrument : `str`
        The instrument name to filter tickets by.
    Returns
    -------
    filtered_tickets : `list[dict]`
        Filtered list of tickets that match the instrument,
        each with an added 'url' field.
    """

    def matches(ticket):
        # Get the list of systems from the object
        obj_system_list = ticket['system']
        search_terms = (instrument, INSTRUMENTS[instrument])
        # Check if any search term appears in any system name
        matched = any(term in system for term in search_terms for system in obj_system_list)
        return matched

    return [ticket for ticket in tickets if matches(ticket)]



def get_jira_tickets(
        dayobs_start: int,
        dayobs_end: int,
        telescope: str
        ) -> list[dict]:
    logger.info(f"Jira service: start: {dayobs_start}, "
          f"end: {dayobs_end} and telescope: {telescope}")

    jira = JiraAdapter(
        max_dayobs=dayobs_end,
        min_dayobs=dayobs_start,
    )
    logger.info(f"max_dayobs: {jira.max_dayobs}, min_dayobs: {jira.min_dayobs}, telescope: {telescope}")
    tickets = jira.fetch_issues()
    if not tickets:
        logger.warning("No Jira tickets found for the specified date range and telescope.")
        return []
    logger.info(f"Found {len(tickets)} Jira tickets.")

    system_tickets = filter_tickets_by_instrument(
        tickets,
        instrument=telescope,
    )
    return system_tickets
