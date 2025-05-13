import os
from datetime import timedelta
from urllib.parse import quote

import requests
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter
from pytz import timezone


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

    ### FROM LOVE
    ## To test get_jira_obs_report:
    # # Get JIRA observation issues
    try:
        report["obs_issues"] = get_jira_obs_report({"day_obs": report["day_obs"]})
    except Exception as e:
        return Response(
            {"error": str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


### FROM LOVE
def get_jira_obs_report(request_data):
    """Connect to the Rubin Observatory JIRA Cloud REST API to
    query all issues of the OBS project for a certain obs day.

    For more information on the REST API endpoints refer to:
    - https://developer.atlassian.com/cloud/jira/platform/rest/v3
    - https://developer.atlassian.com/cloud/jira/platform/\
        basic-auth-for-rest-apis/

    Parameters
    ----------
    request_data : `dict`
        The request data

    Notes
    -----
    The JIRA REST API query is based on the user timezone so
    we need to account for the timezone difference between the user and the
    server. The user timezone is obtained from the JIRA API.

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
    # THink about dayobs
    intitial_day_obs_tai = (
        self.min_dayobs
    )  # get_obsday_to_tai(request_data.get("day_obs"))
    final_day_obs_tai = self.max_dayobs  # intitial_day_obs_tai + timedelta(days=1)

    headers = {
        "Authorization": f"Basic {os.environ.get('JIRA_API_TOKEN')}",
        "content-type": "application/json",
    }

    # Get user timezone
    url = f"https://{os.environ.get('JIRA_API_HOSTNAME')}/rest/api/latest/myself"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        user_timezone = timezone(response.json()["timeZone"])
    else:
        raise Exception(
            f"Error getting user timezone from {os.environ.get('JIRA_API_HOSTNAME')}"
        )

    # Think about dayobs
    start_date_user_datetime = intitial_day_obs_tai.replace(
        tzinfo=timezone("UTC")
    ).astimezone(user_timezone)
    end_date_user_datetime = final_day_obs_tai.replace(
        tzinfo=timezone("UTC")
    ).astimezone(user_timezone)

    # Think about dayobs
    initial_day_obs_string = start_date_user_datetime.strftime("%Y-%m-%d")
    final_day_obs_string = end_date_user_datetime.strftime("%Y-%m-%d")
    start_date_user_time_string = start_date_user_datetime.time().strftime("%H:%M")
    end_date_user_time_string = end_date_user_datetime.time().strftime("%H:%M")

    # JQL query to find issues created on a specific date
    jql_query = (
        f"project = 'OBS' "
        f"AND created >= '{initial_day_obs_string} {start_date_user_time_string}' "
        f"AND created <= '{final_day_obs_string} {end_date_user_time_string}'"
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
                # Add status, do we need other fields?
                "status": issue["fields"]["status"].split(".")[0],
            }
            for issue in issues
        ]
    raise Exception(f"Error getting issues from {os.environ.get('JIRA_API_HOSTNAME')}")


# Do we need to parse into html?
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
