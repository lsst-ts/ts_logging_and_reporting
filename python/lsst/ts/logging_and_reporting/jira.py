"""
For more information on the REST API endpoints refer to:
- https://developer.atlassian.com/cloud/jira/platform/rest/v3
- https://developer.atlassian.com/cloud/jira/platform/\
    basic-auth-for-rest-apis/
"""

import traceback
from datetime import datetime
from urllib.parse import quote

import requests
from pytz import timezone

import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.utils as ut

OBS_SYSTEMS_FIELD = "customfield_10476"
TIME_LOST_FIELD = "customfield_10106"

dayobs_str_format = "%Y-%m-%d %H:%M"
timestamp_input_format = "%Y-%m-%dT%H:%M:%S.%f%z"
timestamp_output_format = "%Y-%m-%d %H:%M:%S"


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


class JiraAdapter:
    EXCLUDED_STATUSES = ["Cancelled"]
    ISSUE_FIELDS = [
        "key",
        "summary",
        "updated",
        "created",
        "status",
        "system",
        OBS_SYSTEMS_FIELD,
        TIME_LOST_FIELD,
    ]

    def __init__(
        self,
        *,
        jira_token=None,
        jira_hostname=None,
    ):
        self.jira_token = jira_token
        self.jira_hostname = jira_hostname
        self.base_url = f"https://{self.jira_hostname}"
        self.headers = {
            "Authorization": f"Basic {self.jira_token}",
            "content-type": "application/json",
        }

    def get_users_timezone(self):
        users_url = f"{self.base_url}/rest/api/latest/myself"
        response = requests.get(users_url, headers=self.headers)
        if response.status_code == 200:
            return timezone(response.json()["timeZone"])
        else:
            raise Exception(
                f"Error getting user timezone from {self.jira_hostname}: "
                f"{response.status_code} - {response.text}"
            )

    def _search(self, jql_query, fields):
        url = f"{self.base_url}/rest/api/latest/search/jql?jql={quote(jql_query)}&fields={fields}"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            msg = f"Error querying Jira: {response.status_code} - {response.text}"
            traceback.print_exc()
            raise ex.BaseLogrepError(msg) from err
        except requests.exceptions.ConnectionError as err:
            msg = f"Error connecting to Jira. {str(err)}"
            traceback.print_exc()
            raise ex.BaseLogrepError(msg) from err

        return response.json().get("issues", [])

    def get_obs_issues(self, min_dayobs, max_dayobs):
        """Query all issues of the OBS project for a specified range
        of observation dates.

        Notes
        -----
        The JIRA REST API query is based on the user timezone so
        we need to specify UTC timezone and we expect min_dayobs
        and max_dayobs to be given in UTC.

        Returns
        -------
        List
            List of issue dictionaries containing the following keys:
            - key: The issue key
            - summary: The issue summary
            - updated: The timestamp of the most recent update
            - created: The issue creation date
            - status: The current status of issue
            - system: The relevant system, e.g. "Simonyi"
            - isNew: True if created within specified range
            - url: The URl of the issue
            - time_lost: The time lost in the issue
        """
        user_timezone = self.get_users_timezone()

        #  A bunch of date formatting that could be wrapped in function
        start_dayobs_utc = ut.get_utc_datetime_from_dayobs_str(min_dayobs)
        end_dayobs_utc = ut.get_utc_datetime_from_dayobs_str(max_dayobs)

        # convert the utc times to user timezone
        start_dayobs_user = start_dayobs_utc.astimezone(user_timezone)
        end_dayobs_user = end_dayobs_utc.astimezone(user_timezone)

        start_dayobs_str = start_dayobs_user.strftime(dayobs_str_format)
        end_dayobs_str = end_dayobs_user.strftime(dayobs_str_format)

        # JQL query to get all issues in the OBS project created between
        # the specified dayobs range, excluding certain statuses
        status_exclusions = " ".join(f'AND status != "{s}"' for s in self.EXCLUDED_STATUSES)
        jql_query = (
            f"project = OBS {status_exclusions} "
            f'AND ((created >= "{start_dayobs_str}" '
            f'AND created < "{end_dayobs_str}") '
            f'OR (updated >= "{start_dayobs_str}" '
            f'AND updated < "{end_dayobs_str}"))'
        )
        fields = ",".join(self.ISSUE_FIELDS)

        issues = self._search(jql_query, fields=fields)

        return [
            {
                "key": issue["key"],
                "summary": issue["fields"]["summary"],
                "updated": datetime.strptime(issue["fields"]["updated"], timestamp_input_format).strftime(
                    timestamp_output_format
                ),
                "created": datetime.strptime(issue["fields"]["created"], timestamp_input_format).strftime(
                    timestamp_output_format
                ),
                "status": issue["fields"]["status"]["name"],
                "system": get_system_names(issue["fields"][OBS_SYSTEMS_FIELD]),
                "isNew": datetime.strptime(issue["fields"]["created"], timestamp_input_format)
                >= start_dayobs_user
                and datetime.strptime(issue["fields"]["created"], timestamp_input_format) < end_dayobs_user,
                "url": f"{self.base_url}/browse/{issue['key']}",
                "time_lost": issue["fields"][TIME_LOST_FIELD],
            }
            for issue in issues
        ]

    def fetch_block_ticket_summaries(self, ticket_keys):
        """
        Fetch summary fields for a list of BLOCK tickets.

        Parameters
        ----------
        ticket_keys : list[str]
            List of Jira issue keys (e.g. ["BLOCK-123", "BLOCK-456"])

        Returns
        -------
        dict
            Mapping of ticket key -> summary
        """
        if not ticket_keys:
            return {}

        keys_str = ",".join(ticket_keys)
        jql_query = f"project = BLOCK AND key in ({keys_str})"

        issues = self._search(jql_query, fields="summary")

        return {issue["key"]: issue["fields"]["summary"] for issue in issues}
