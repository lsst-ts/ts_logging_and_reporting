from unittest.mock import Mock, patch

import pytest
import requests

from lsst.ts.logging_and_reporting.adapters.jira_block import JiraBlockAdapter
from lsst.ts.logging_and_reporting.cache_ttl import MUTABLE_TTL_REDIS

SERVER = "https://jira.test"


def block_requests_get(summaries):
    """Respond to BLOCK searches with the given key -> summary map."""

    def respond(url, **kwargs):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "issues": [{"key": key, "fields": {"summary": summary}} for key, summary in summaries.items()]
        }
        return response

    return Mock(side_effect=respond)


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")
    return JiraBlockAdapter(fake_redis, server_url=SERVER)


class TestFetchByIds:
    def test_returns_summaries_keyed_by_id(self, adapter):
        mock_get = block_requests_get({"BLOCK-1": "First block", "BLOCK-2": "Second block"})
        with patch("requests.Session.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-1", "BLOCK-2"])
        assert result == {"BLOCK-1": "First block", "BLOCK-2": "Second block"}

    def test_jql_query_and_fields(self, adapter):
        mock_get = block_requests_get({"BLOCK-1": "First block", "BLOCK-2": "Second block"})
        with patch("requests.Session.get", mock_get):
            adapter.fetch_by_ids(["BLOCK-1", "BLOCK-2"])
        params = mock_get.call_args.kwargs["params"]
        assert params["jql"] == "project = BLOCK AND key in (BLOCK-1,BLOCK-2)"
        assert params["fields"] == "summary"

    def test_unreturned_key_yields_none(self, adapter):
        mock_get = block_requests_get({"BLOCK-1": "First block"})
        with patch("requests.Session.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-1", "BLOCK-404"])
        assert result == {"BLOCK-1": "First block", "BLOCK-404": None}

    def test_basic_auth_headers(self, adapter):
        mock_get = block_requests_get({})
        with patch("requests.Session.get", mock_get):
            adapter.fetch_by_ids(["BLOCK-1"])
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Basic test-token"
        assert headers["content-type"] == "application/json"

    def test_http_error_propagates(self, adapter):
        response = Mock()
        response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")
        with patch("requests.Session.get", return_value=response):
            with pytest.raises(requests.HTTPError):
                adapter.fetch_by_ids(["BLOCK-1"])


class TestCaching:
    def test_cached_ids_are_not_refetched(self, adapter):
        with patch("requests.Session.get", block_requests_get({"BLOCK-1": "First block"})):
            adapter.fetch_by_ids(["BLOCK-1"])
        mock_get = block_requests_get({})
        with patch("requests.Session.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-1"])
        assert result == {"BLOCK-1": "First block"}
        mock_get.assert_not_called()

    def test_only_missing_ids_are_fetched(self, adapter):
        with patch("requests.Session.get", block_requests_get({"BLOCK-1": "First block"})):
            adapter.fetch_by_ids(["BLOCK-1"])
        mock_get = block_requests_get({"BLOCK-2": "Second block"})
        with patch("requests.Session.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-1", "BLOCK-2"])
        assert result == {"BLOCK-1": "First block", "BLOCK-2": "Second block"}
        assert mock_get.call_args.kwargs["params"]["jql"] == "project = BLOCK AND key in (BLOCK-2)"

    def test_none_result_is_cached(self, adapter):
        with patch("requests.Session.get", block_requests_get({})):
            adapter.fetch_by_ids(["BLOCK-404"])
        mock_get = block_requests_get({})
        with patch("requests.Session.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-404"])
        assert result == {"BLOCK-404": None}
        mock_get.assert_not_called()

    def test_entries_stored_with_mutable_ttl(self, adapter, fake_redis):
        with patch("requests.Session.get", block_requests_get({"BLOCK-1": "First block"})):
            adapter.fetch_by_ids(["BLOCK-1"])
        assert fake_redis.ttls["adapter:jira_block:BLOCK-1"] == MUTABLE_TTL_REDIS
