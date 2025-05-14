import os
import requests
from urllib.parse import quote

from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter


class JiraAdapter(SourceAdapter):
    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,
        max_dayobs=None,
        limit=None,
        verbose=False,
        warning=True,
    ):
        super().__init__(
            server_url=None,
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

        start_dayobs_str = self.min_dayobs.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_dayobs_str = self.max_dayobs.strftime("%Y-%m-%dT%H:%M:%SZ")

        jql_query = (
            f'project = OBS AND created >= "{start_dayobs_str}" '
            f'AND created < "{end_dayobs_str}"'
        )

        url = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/rest/api/latest/search?jql={quote(jql_query)}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            issues = response.json()["issues"]
            return [
                {
                    "key": issue["key"],
                    "summary": issue["fields"]["summary"],
                    "reporter": issue["fields"]["creator"]["displayName"],
                    "created": issue["fields"]["created"].split(".")[0],
                    "status": issue["fields"]["status"]["name"],
                }
                for issue in issues
            ]
        raise Exception(
            f"Error getting issues from {os.environ.get('JIRA_API_HOSTNAME')}"
        )

    # Do we need to parse into html?
    @staticmethod
    def parse_obs_issues_array_to_plain_text(obs_issues):
        """Parse the OBS issues array to plain text.

        Parameters
        ----------
        obs_issues : `list`
            List of OBS issues

        Notes
        -----
        Each element of the obs_issues list must be dictionary
        with the following keys:
        - key: The key of the issue
        - summary: The summary of the issue
        - time_lost: The time lost in hours
        - reporter: The reporter of the issue
        - created: The creation date of the issue

        If a key is missing, it will be replaced by a "None".

        Returns
        -------
        str
            The OBS issues in plain text format
        """

        plain_text = ""
        for issue in obs_issues:
            plain_text += f"{issue.get('key')} - {issue.get('summary')}: "
            plain_text += f"Created by {issue.get('reporter')}\n"

        return plain_text
