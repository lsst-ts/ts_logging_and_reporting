import datetime
import json

from pydantic import BaseModel


class JiraTicket(BaseModel):
    url: str
    summary: str
    updated: datetime.datetime
    create: datetime.datetime
    system: str
    status: str
    key: str


def extract_ticket_info(tic) -> JiraTicket:
    return {
        "url": f"https://rubinobs.atlassian.net/browse/{tic["key"]}",
        "summary": tic["fields"]["summary"],
        "updated": tic["fields"]["updated"],
        "created": tic["fields"]["created"],
        "status": tic["fields"]["status"]["name"],
        "key": tic["key"]
    }


def get_jira_tickets(
        dayobs_start: datetime.date,
        dayobs_end: datetime.date,
        telescope: str
        ) -> list[JiraTicket]:
    print(f"Getting Jira tickets for start: {dayobs_start}, "
          f"end: {dayobs_end} and telescope: {telescope}")
    # TODO: replace with code to retrieve
    # ticket information using data adaptors
    with open("data/jira-tickets.json") as f:
        content = json.load(f)
        tickets = [{
            "url": f"https://rubinobs.atlassian.net/browse/{tic["key"]}",
            "summary": tic["fields"]["summary"],
            "updated": tic["fields"]["updated"],
            "created": tic["fields"]["created"],
            "status": tic["fields"]["status"]["name"],
            "system": telescope,
            "key": tic["key"]
        } for tic in content['issues']]
        # tickets = list(map(extract_ticket_info, content['issues']))
    return tickets
