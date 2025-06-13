import datetime
import os
import logging

from pydantic import BaseModel
from lsst.ts.logging_and_reporting.jira import JiraAdapter


logger = logging.getLogger(__name__)


class JiraTicket(BaseModel):
    url: str
    summary: str
    updated: datetime.datetime
    create: datetime.datetime
    system: str
    status: str
    key: str



def get_jira_tickets(
        dayobs_start: int,
        dayobs_end: int,
        telescope: str
        ) -> list[JiraTicket]:
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
    # Convert the tickets to a list of JiraTicket models
    tickets = [
        JiraTicket(
            url=f"https://{os.environ.get('JIRA_API_HOSTNAME')}/browse/{ticket.get('key')}",
            summary=ticket.get('summary'),
            updated=ticket.get('updated'),
            create=ticket.get('created'),
            system=ticket.get('system'),
            status=ticket.get('status'),
            key=ticket.get('key')
        ) for ticket in tickets
    ]
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
    return tickets
