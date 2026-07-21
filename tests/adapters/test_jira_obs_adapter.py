from unittest.mock import Mock, patch

import pytest
import requests

from lsst.ts.logging_and_reporting.adapters.jira_obs import (
    OBS_SYSTEMS_FIELD,
    TIME_LOST_FIELD,
    JiraObsCachedAdapter,
)
from lsst.ts.logging_and_reporting.adapters.mixins import JiraApiMixin
from lsst.ts.logging_and_reporting.cache_ttl import MUTABLE_TTL_REDIS

SERVER = "https://jira.test"


def make_issue(
    key="OBS-1",
    created="2025-01-01T18:00:00.000+0000",
    updated=None,
    systems=None,
    status="In Progress",
    time_lost=0.5,
):
    return {
        "key": key,
        "fields": {
            "summary": f"summary of {key}",
            "created": created,
            "updated": updated or created,
            "status": {"name": status},
            OBS_SYSTEMS_FIELD: systems if systems is not None else [{"name": "Simonyi"}],
            TIME_LOST_FIELD: time_lost,
        },
    }


def jira_requests_get(issues, tz="UTC"):
    """Route /myself to the timezone payload and searches to issues."""

    def respond(url, **kwargs):
        response = Mock()
        response.raise_for_status.return_value = None
        if url.endswith("/myself"):
            response.json.return_value = {"timeZone": tz}
        else:
            response.json.return_value = {"issues": issues}
        return response

    return Mock(side_effect=respond)


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")
    return JiraObsCachedAdapter(fake_redis, server_url=SERVER)


class TestGetSystemNames:
    def test_extracts_names_from_nested_structure(self):
        field = [{"name": "Simonyi", "child": {"name": "M1M3"}}, [{"name": "AuxTel"}]]
        assert JiraApiMixin.get_system_names(field) == ["Simonyi", "M1M3", "AuxTel"]

    def test_none_field_gives_empty_list(self):
        assert JiraApiMixin.get_system_names(None) == []


class TestFetchFromSource:
    def test_buckets_ticket_by_created_dayobs(self, adapter):
        issues = [make_issue(created="2025-01-01T18:00:00.000+0000")]
        with patch("requests.get", jira_requests_get(issues)):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert [t["key"] for t in result[20250101]] == ["OBS-1"]
        assert result[20250102] == []

    def test_ticket_appears_in_created_and_updated_buckets(self, adapter):
        issues = [
            make_issue(
                created="2025-01-01T18:00:00.000+0000",
                updated="2025-01-02T18:00:00.000+0000",
            )
        ]
        with patch("requests.get", jira_requests_get(issues)):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert [t["key"] for t in result[20250101]] == ["OBS-1"]
        assert [t["key"] for t in result[20250102]] == ["OBS-1"]

    def test_out_of_range_buckets_dropped(self, adapter):
        issues = [make_issue(created="2025-01-05T18:00:00.000+0000")]
        with patch("requests.get", jira_requests_get(issues)):
            result = adapter._fetch_from_source([20250101])
        assert result == {20250101: []}

    def test_one_search_per_contiguous_run(self, adapter):
        mock_get = jira_requests_get([])
        with patch("requests.get", mock_get):
            adapter._fetch_from_source([20250101, 20250102, 20250105])
        urls = [call.args[0] for call in mock_get.call_args_list]
        assert urls.count(f"{SERVER}/rest/api/latest/myself") == 1
        assert urls.count(f"{SERVER}/rest/api/latest/search/jql") == 2

    def test_jql_window_and_fields(self, adapter):
        mock_get = jira_requests_get([])
        with patch("requests.get", mock_get):
            adapter._fetch_from_source([20250101, 20250102])
        params = mock_get.call_args.kwargs["params"]
        jql = params["jql"]
        assert "project = OBS" in jql
        assert 'status != "Cancelled"' in jql
        assert 'created >= "2025-01-01 12:00"' in jql
        assert 'created < "2025-01-03 12:00"' in jql
        assert 'updated >= "2025-01-01 12:00"' in jql
        assert params["fields"].startswith("key,summary,updated,created,status")

    def test_jql_dates_converted_to_user_timezone(self, adapter):
        mock_get = jira_requests_get([], tz="Etc/GMT+3")
        with patch("requests.get", mock_get):
            adapter._fetch_from_source([20250101])
        jql = mock_get.call_args.kwargs["params"]["jql"]
        assert 'created >= "2025-01-01 09:00"' in jql

    def test_record_shape(self, adapter):
        issues = [
            make_issue(
                created="2025-01-01T18:30:45.123+0000",
                updated="2025-01-01T20:00:00.000+0000",
                systems=[{"name": "Simonyi", "child": {"name": "M1M3"}}],
            )
        ]
        with patch("requests.get", jira_requests_get(issues)):
            result = adapter._fetch_from_source([20250101])
        record = result[20250101][0]
        assert record == {
            "key": "OBS-1",
            "summary": "summary of OBS-1",
            "updated": "2025-01-01 20:00:00",
            "created": "2025-01-01 18:30:45",
            "status": "In Progress",
            "system": ["Simonyi", "M1M3"],
            "url": f"{SERVER}/browse/OBS-1",
            "time_lost": 0.5,
            "created_utc": "2025-01-01T18:30:45.123000+00:00",
        }

    def test_basic_auth_headers(self, adapter):
        mock_get = jira_requests_get([])
        with patch("requests.get", mock_get):
            adapter._fetch_from_source([20250101])
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Basic test-token"
        assert headers["content-type"] == "application/json"

    def test_user_timezone_fetched_once_across_fetches(self, adapter):
        mock_get = jira_requests_get([])
        with patch("requests.get", mock_get):
            adapter._fetch_from_source([20250101])
            adapter._fetch_from_source([20250102])
        urls = [call.args[0] for call in mock_get.call_args_list]
        assert urls.count(f"{SERVER}/rest/api/latest/myself") == 1

    def test_http_error_propagates(self, adapter):
        response = Mock()
        response.raise_for_status.side_effect = requests.HTTPError("502 Bad Gateway")
        with patch("requests.get", return_value=response):
            with pytest.raises(requests.HTTPError):
                adapter._fetch_from_source([20250101])


class TestTtl:
    def test_mutable_ttl_for_historical_dayobs(self, adapter, fake_redis):
        issues = [make_issue(created="2020-01-01T18:00:00.000+0000")]
        with patch("requests.get", jira_requests_get(issues)):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:jira_obs:20200101"] == MUTABLE_TTL_REDIS
