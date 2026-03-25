from datetime import datetime
from unittest.mock import Mock, patch
from urllib.parse import quote

import pytest
from pytz import UTC
from requests.exceptions import ConnectionError, HTTPError

from lsst.ts.logging_and_reporting.jira import (
    OBS_SYSTEMS_FIELD,
    TIME_LOST_FIELD,
    JiraAdapter,
    JiraClient,
    ex,
    get_system_names,
)


# ------------------------
# Tests for get_system_names (from jira returned string)
# ------------------------
@pytest.mark.parametrize(
    "input_data,expected",
    [
        (
            [[{"name": "Simonyi", "id": "0", "children": ["15", "16", "17", "18"]}]],
            ["Simonyi"],
        ),
        ([[]], []),
        (
            [[{"name": "Simonyi", "children": [{"name": "SubSystem1"}]}, {"id": "42"}]],
            ["Simonyi", "SubSystem1"],
        ),
        (
            [
                [
                    {
                        "name": "Control and Monitoring Software",
                        "id": "8",
                        "children": [
                            "560",
                            "78",
                            "86",
                            "125",
                        ],
                    }
                ],
                [{"name": "Scheduler", "id": "79", "children": ["469", "470", "471"]}],
                [{"name": "Simonyi Scheduler CSC", "id": "470"}],
            ],
            ["Control and Monitoring Software", "Scheduler", "Simonyi Scheduler CSC"],
        ),
    ],
)
def test_get_system_names(input_data, expected):
    assert get_system_names(input_data) == expected


# ------------------------
# Tests for get_users_timezone
# ------------------------
@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_get_users_timezone_success(mock_requests_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"timeZone": "UTC"}
    mock_requests_get.return_value = mock_response

    adapter = JiraAdapter(jira_token="token", jira_hostname="host")
    tz = adapter.get_users_timezone()
    assert str(tz) == "UTC"
    mock_requests_get.assert_called_once_with("https://host/rest/api/latest/myself", headers=adapter.headers)


@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_get_users_timezone_failure(mock_requests_get):
    mock_response = Mock()
    mock_response.status_code = 403
    mock_response.text = "Forbidden"
    mock_requests_get.return_value = mock_response

    adapter = JiraAdapter(jira_token="token", jira_hostname="host")
    with pytest.raises(Exception, match="Error getting user timezone"):
        adapter.get_users_timezone()


# ------------------------
# Tests for _search
# ------------------------
@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_search_success(mock_requests_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"issues": [{"key": "OBS-1", "fields": {}}]}
    mock_requests_get.return_value = mock_response

    adapter = JiraAdapter(jira_token="token", jira_hostname="host")
    result = adapter._search("project=OBS", fields="key,summary")
    assert result == [{"key": "OBS-1", "fields": {}}]
    mock_requests_get.assert_called_once()


@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_search_http_error(mock_requests_get):
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Server error"
    mock_response.raise_for_status.side_effect = HTTPError()
    mock_requests_get.return_value = mock_response

    adapter = JiraAdapter(jira_token="token", jira_hostname="host")
    with pytest.raises(ex.BaseLogrepError, match="Error querying Jira"):
        adapter._search("project=OBS", fields="key")


@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_search_connection_error(mock_requests_get):
    mock_requests_get.side_effect = ConnectionError("Network down")

    adapter = JiraAdapter(jira_token="token", jira_hostname="host")
    with pytest.raises(ex.BaseLogrepError, match="Error connecting to Jira"):
        adapter._search("project=OBS", fields="key")


# ------------------------
# Tests for get_obs_issues (mocked)
# ------------------------
@pytest.fixture
def sample_jira_issues():
    """Return a list of mock Jira issues for testing."""
    return [
        {
            "key": "OBS-999",
            "fields": {
                "summary": "Test issue",
                "updated": "2025-01-01T12:00:00.000+0000",
                "created": "2025-01-01T11:00:00.000+0000",
                "status": {"name": "Open"},
                OBS_SYSTEMS_FIELD: [[{"name": "Simonyi"}]],
                TIME_LOST_FIELD: 2.0,
            },
        },
        {
            "key": "OBS-998",
            "fields": {
                "summary": "Test issue 2",
                "updated": "2025-01-01T14:00:00.000+0000",
                "created": "2025-01-01T13:00:00.000+0000",
                "status": {"name": "To Do"},
                OBS_SYSTEMS_FIELD: [[{"name": "AuxTel"}]],
                TIME_LOST_FIELD: None,
            },
        },
    ]


@patch("lsst.ts.logging_and_reporting.jira.requests.get")
@patch("lsst.ts.logging_and_reporting.jira.ut.get_utc_datetime_from_dayobs_str")
def test_get_obs_issues(mock_get_utc, mock_requests_get, sample_jira_issues):
    # Set up mock UTC conversion
    mock_get_utc.side_effect = [
        datetime(2025, 1, 1, 12, 0, tzinfo=UTC),  # for min_dayobs
        datetime(2025, 1, 2, 12, 0, tzinfo=UTC),  # for max_dayobs
    ]

    # /myself response for timezone
    mock_response_myself = Mock()
    mock_response_myself.status_code = 200
    mock_response_myself.json.return_value = {"timeZone": "UTC"}

    # /search/jql response
    mock_response_search = Mock()
    mock_response_search.status_code = 200
    mock_response_search.json.return_value = {"issues": sample_jira_issues}

    # Side effect order: first call = /myself, second call = /search/jql
    mock_requests_get.side_effect = [mock_response_myself, mock_response_search]

    adapter = JiraAdapter(jira_token="token", jira_hostname="host")
    result = adapter.get_obs_issues(min_dayobs="20250101", max_dayobs="20250102")

    # Verify the Jira JQL query included the excluded statuses
    # called_url = mock_requests_get.call_args.args[0]
    called_url = mock_requests_get.call_args_list[1][0][0]  # second call to /search/jql
    status_exclusions = " ".join(f'AND status != "{s}"' for s in adapter.EXCLUDED_STATUSES)
    assert quote(status_exclusions) in called_url

    # Validate returned issues
    assert isinstance(result, list)
    assert result[0]["key"] == "OBS-999"
    assert result[0]["system"] == ["Simonyi"]
    assert result[0]["updated"] == "2025-01-01 12:00:00"
    assert result[0]["time_lost"] == 2.0
    assert result[1]["key"] == "OBS-998"
    assert result[1]["system"] == ["AuxTel"]
    assert result[1]["updated"] == "2025-01-01 14:00:00"
    assert result[1]["time_lost"] is None


@pytest.fixture
def sample_jira_issues_at_dayobs_boundary():
    """Return a list of mock Jira issues for testing the boundary
    between dayobs for the returned isNew flag.
    """
    return [
        {
            "key": "OBS-START",
            "fields": {
                "summary": "At start",
                "updated": "2025-01-01T12:30:00.000+0000",
                "created": "2025-01-01T12:00:00.000+0000",  # exactly at start
                "status": {"name": "Open"},
                OBS_SYSTEMS_FIELD: [[{"name": "Simonyi"}]],
                TIME_LOST_FIELD: None,
            },
        },
        {
            "key": "OBS-END",
            "fields": {
                "summary": "At end",
                "updated": "2025-01-01T12:30:00.000+0000",
                "created": "2025-01-01T13:00:00.000+0000",  # exactly at end
                "status": {"name": "Open"},
                OBS_SYSTEMS_FIELD: [[{"name": "AuxTel"}]],
                TIME_LOST_FIELD: None,
            },
        },
        {
            "key": "OBS-MID",
            "fields": {
                "summary": "In between",
                "updated": "2025-01-01T12:30:00.000+0000",
                "created": "2025-01-01T12:30:00.000+0000",  # between start & end
                "status": {"name": "Open"},
                OBS_SYSTEMS_FIELD: [[{"name": "AuxTel"}]],
                TIME_LOST_FIELD: None,
            },
        },
        {
            "key": "OBS-BEFORE",
            "fields": {
                "summary": "Before start",
                "updated": "2025-01-01T11:59:59.000+0000",
                "created": "2025-01-01T11:59:59.000+0000",  # before start
                "status": {"name": "Open"},
                OBS_SYSTEMS_FIELD: [[{"name": "AuxTel"}]],
                TIME_LOST_FIELD: None,
            },
        },
    ]


@patch("lsst.ts.logging_and_reporting.jira.requests.get")
@patch("lsst.ts.logging_and_reporting.jira.ut.get_utc_datetime_from_dayobs_str")
def test_get_obs_issues_is_new_boundaries(
    mock_get_utc,
    mock_requests_get,
    sample_jira_issues_at_dayobs_boundary,
):
    from datetime import datetime

    from pytz import UTC

    # Set min/max dayobs
    start_utc = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    end_utc = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    mock_get_utc.side_effect = [start_utc, end_utc]

    # Mock /myself returns UTC timezone
    mock_response_myself = Mock()
    mock_response_myself.status_code = 200
    mock_response_myself.json.return_value = {"timeZone": "UTC"}

    mock_response_search = Mock()
    mock_response_search.status_code = 200
    mock_response_search.json.return_value = {"issues": sample_jira_issues_at_dayobs_boundary}

    mock_requests_get.side_effect = [mock_response_myself, mock_response_search]

    adapter = JiraAdapter(jira_token="abc123", jira_hostname="fake.jira.com")
    result = adapter.get_obs_issues(min_dayobs="20250101", max_dayobs="20250101")

    # Check isNew field for boundary conditions
    assert result[0]["key"] == "OBS-START"
    assert result[0]["isNew"] is True  # exactly start -> True

    assert result[1]["key"] == "OBS-END"
    assert result[1]["isNew"] is False  # exactly end -> False

    assert result[2]["key"] == "OBS-MID"
    assert result[2]["isNew"] is True  # in between -> True

    assert result[3]["key"] == "OBS-BEFORE"
    assert result[3]["isNew"] is False  # before start -> False


# ------------------------
# Tests for fetch_block_ticket_summaries
# ------------------------
@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_fetch_block_ticket_summaries_success(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {
        "issues": [
            {"key": "BLOCK-1", "fields": {"summary": "First ticket"}},
            {"key": "BLOCK-2", "fields": {"summary": "Second ticket"}},
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    client = JiraClient()
    result = client.fetch_block_ticket_summaries(["BLOCK-1", "BLOCK-2"])

    assert result == {
        "BLOCK-1": "First ticket",
        "BLOCK-2": "Second ticket",
    }


def test_fetch_block_ticket_summaries_empty_input():
    client = JiraClient()
    result = client.fetch_block_ticket_summaries([])

    assert result == {}


@patch("lsst.ts.logging_and_reporting.jira.requests.get")
def test_search_http_error(mock_get):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("HTTP error")
    mock_response.status_code = 500
    mock_response.text = "Internal error"
    mock_get.return_value = mock_response

    client = JiraClient()

    with pytest.raises(Exception):  # optionally ex.BaseLogrepError if imported
        client._search("some jql", "summary")
