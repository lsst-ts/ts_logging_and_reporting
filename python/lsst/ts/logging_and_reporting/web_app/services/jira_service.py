import os
import logging

from lsst.ts.logging_and_reporting.jira import JiraAdapter


logger = logging.getLogger(__name__)


INSTRUMENTS = {
    "LATISS": "AuxTel",
    "LSSTCam" : "Simonyi",
}


def filter_tickets_by_instrument(tickets, instrument):

    def matches_and_add_url(ticket):
        # Get the list of systems from the object
        obj_system_list = ticket['system']
        search_terms = (instrument, INSTRUMENTS[instrument])
        # Check if any search term appears in any system name
        matched = any(term in system for term in search_terms for system in obj_system_list)
        if matched:
            ticket['url'] = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/browse/{ticket.get('key')}"
            return True
        return False

    return [ticket for ticket in tickets if matches_and_add_url(ticket)]


# class JiraTicket(BaseModel):
#     url: str
#     summary: str
#     updated: datetime.datetime
#     create: datetime.datetime
#     system: list[str]
#     status: str
#     key: str



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
    # Convert the tickets to a list of JiraTicket models
    # tickets = [
    #     JiraTicket(
    #         url=f"https://{os.environ.get('JIRA_API_HOSTNAME')}/browse/{ticket.get('key')}",
    #         summary=ticket.get('summary'),
    #         updated=ticket.get('updated'),
    #         create=ticket.get('created'),
    #         system=ticket.get('system'),
    #         status=ticket.get('status'),
    #         key=ticket.get('key')
    #     ) for ticket in tickets if ticket.get('system') == telescope
    # ]
    # with open("data/jira-tickets.json") as f:
    #     content = json.load(f)
    #     tickets = [{
    #         "url": f"https://rubinobs.atlassian.net/browse/{tic["key"]}",
    #         "summary": tic["fields"]["summary"],
    #         "updated": tic["fields"]["updated"],
    #         "created": tic["fields"]["created"],
    #         "status": tic["fields"]["status"]["name"],
    #         "system": telescope,
    #         "key": tic["key"]
    #     } for tic in content['issues']]
    return system_tickets
