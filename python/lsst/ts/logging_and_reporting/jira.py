import os
import requests
from urllib.parse import quote
from pytz import timezone

from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter
import lsst.ts.logging_and_reporting.utils as ut


OBS_SYSTEMS_FIELD = "customfield_10476"


def get_system_names(jira_system_field):
    """Jira returns the value of OBS_SYSTEMS_FIELD in a list of list of dicts,
    where we only care about the dictionary key and value 'name':'Simonyi'
    or other System or subsystem"""
    systems = []

    def walk(obj):
        if isinstance(obj, dict):
            if "name" in obj:
                systems.append(obj["name"])
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(jira_system_field)
    return systems


class JiraAdapter(SourceAdapter):
    def __init__(
        self,
        *,
        min_dayobs=None,
        max_dayobs=None,
        limit=None,
        verbose=False,
        warning=True,
    ):
        super().__init__(
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
            warning=warning,
        )
        self.issues = None

    def fetch_issues(self):
        """Query JIRA issues for the configured dayobs range, save and
        return them via self.issues."""
        if self.issues is None:
            self.issues = self.get_jira_obs_report()
        return self.issues

    def get_jira_obs_report(self):
        """From LOVE-Manager, connect to the Rubin Observatory JIRA Cloud
        REST API to query all issues of the OBS project for a certain obs day.

        For more information on the REST API endpoints refer to:
        - https://developer.atlassian.com/cloud/jira/platform/rest/v3
        - https://developer.atlassian.com/cloud/jira/platform/\
            basic-auth-for-rest-apis/

        Notes
        -----
        The JIRA REST API query is based on the user timezone so
        we need to specify UTC timezone and we expect max_dayobs
        and min_dayobs to be given in UTC.

        Returns
        -------
        List
            List of dictionaries containing the following keys:
            - key: The issue key
            - summary: The issue summary
            - time_lost: The time lost in the issue
            - reporter: The issue reporter
            - created: The issue creation date
        """

        headers = {
            "Authorization": f"Basic {os.environ.get('JIRA_API_TOKEN')}",
            "content-type": "application/json",
        }
        # needs to be tai, is not yet tai
        start_dayobs_utc = ut.get_utc_datetime_from_dayobs_str(self.min_dayobs)
        end_dayobs_utc = ut.get_utc_datetime_from_dayobs_str(self.max_dayobs)

        # Get user's timezone
        url = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/rest/api/latest/myself"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            user_timezone = timezone(response.json()["timeZone"])
        else:
            raise Exception(
                f"Error getting user timezone from {os.environ.get('JIRA_API_HOSTNAME')}: "
                f"{response.status_code} - {response.text}"
            )

        # convert the utc times to user timezone
        start_dayobs_user = start_dayobs_utc.astimezone(user_timezone)
        end_dayobs_user = end_dayobs_utc.astimezone(user_timezone)

        start_dayobs_str = start_dayobs_user.strftime("%Y-%m-%d %H:%M")
        end_dayobs_str = end_dayobs_user.strftime("%Y-%m-%d %H:%M")

        jql_query = (
            f'project = OBS AND (created >= "{start_dayobs_str}" '
            f'AND created < "{end_dayobs_str}")'
        )
        fields = f"key,summary,updated,created,status,system,{OBS_SYSTEMS_FIELD}"
        url = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/rest/api/latest/search/jql?jql={quote(jql_query)}&fields={fields}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            issues = response.json()["issues"]
            return [
                {
                    "key": issue["key"],
                    "summary": issue["fields"]["summary"],
                    "updated": issue["fields"]["updated"],
                    "created": issue["fields"]["created"].split(".")[0],
                    "status": issue["fields"]["status"]["name"],
                    "system": get_system_names(issue["fields"][OBS_SYSTEMS_FIELD]),
                }
                for issue in issues
            ]
        raise Exception(
            f"Error getting issues from {os.environ.get('JIRA_API_HOSTNAME')}: "
            f"{response.status_code} - {response.text}"
        )

    @staticmethod
    def parse_obs_issues_array_to_plain_text(obs_issues):
        """Parse the OBS issues array to plain text.

        Parameters
        ----------
        obs_issues : `list`
            List of OBS issues

        Notes
        -----
        Each list entry (issue) of obs_issues must be dictionary
        with the following keys:
        - key: The key of the issue
        - status: The status of the issue
        - summary: The summary of the issue
        - created: The creation datetime of the issue
        - updated: The last updated datetime of the issue
        - system: The system "customfield_10476" of the issue

        If a key is missing, it will be replaced by a "None".

        Returns
        -------
        str
            The OBS issues in plain text format
        """
        lines = []
        for issue in obs_issues:
            lines.append(
                f"{issue.get('key')} - {issue.get('status')} - {issue.get('summary')} "
                f"Created: {issue.get('created')}, Last Updated: {issue.get('updated')}, "
                f"Impacting: {issue.get('system')}"
            )
        return "\n".join(lines)
