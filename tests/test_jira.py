from unittest.mock import Mock, patch
from urllib.parse import quote

import pytest

from lsst.ts.logging_and_reporting.jira import JiraAdapter, get_system_names


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
# Tests for get_jira_obs_report (mocked)
# ------------------------
@patch("lsst.ts.logging_and_reporting.jira.requests.get")
@patch.dict(
    "lsst.ts.logging_and_reporting.jira.os.environ",
    {"JIRA_API_TOKEN": "abc123", "JIRA_API_HOSTNAME": "fake.jira.com"},
)
@patch("lsst.ts.logging_and_reporting.jira.ut.get_utc_datetime_from_dayobs_str")
def test_get_jira_obs_report(mock_get_utc, mock_requests_get):
    from datetime import datetime

    from pytz import UTC

    # Set up mock UTC conversion
    mock_get_utc.side_effect = [
        datetime(2025, 1, 1, 12, 0, tzinfo=UTC),  # for min_dayobs
        datetime(2025, 1, 2, 12, 0, tzinfo=UTC),  # for max_dayobs
    ]

    # Mock first get request to jira /myself to get timezone
    mock_response_myself_timezone = Mock()
    mock_response_myself_timezone.status_code = 200
    mock_response_myself_timezone.json.return_value = {"timeZone": "UTC"}

    # Second get request to /search/jql
    mock_response_search = Mock()
    mock_response_search.status_code = 200
    mock_response_search.json.return_value = {
        "issues": [
            {
                "key": "OBS-999",
                "fields": {
                    "summary": "Test issue",
                    "updated": "2025-01-01T12:00:00.000Z",
                    "created": "2025-01-01T11:00:00.000Z",
                    "status": {"name": "Open"},
                    "customfield_10476": [[{"name": "Simonyi"}]],
                },
            },
        ]
    }

    # Setup side effects for each get call
    mock_requests_get.side_effect = [
        mock_response_myself_timezone,
        mock_response_search,
    ]

    adapter = JiraAdapter(min_dayobs="20250101", max_dayobs="20250102")
    result = adapter.get_jira_obs_report()

    # Verify that the Jira JQL query included the excluded statuses
    called_url = mock_requests_get.call_args.args[0]
    status_exclusions = " ".join(f'AND status != "{s}"' for s in adapter.EXCLUDED_STATUSES)
    assert quote(status_exclusions) in called_url

    assert isinstance(result, list)
    assert result[0]["key"] == "OBS-999"
    assert result[0]["system"] == ["Simonyi"]
    assert result[0]["updated"] == "2025-01-01 12:00:00"
