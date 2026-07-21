from unittest.mock import Mock, patch

import pytest
import requests

from lsst.ts.logging_and_reporting.adapters.zephyr import ZephyrAdapter
from lsst.ts.logging_and_reporting.cache_ttl import MUTABLE_TTL_REDIS

SERVER = "https://zephyr.test/v2"


def zephyr_requests_get(test_cases):
    """Route GET /testcases/{key} to canned names; unknown keys 404."""

    def respond(url, **kwargs):
        key = url.rsplit("/", 1)[-1]
        response = Mock()
        if key in test_cases:
            response.raise_for_status.return_value = None
            response.json.return_value = {"key": key, "name": test_cases[key]}
        else:
            response.status_code = 404
            response.raise_for_status.side_effect = requests.HTTPError("404 Not Found", response=response)
        return response

    return Mock(side_effect=respond)


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("ZEPHYR_API_TOKEN", "test-token")
    return ZephyrAdapter(fake_redis, server_url=SERVER)


class TestFetchByIds:
    def test_returns_names_keyed_by_requested_key(self, adapter):
        mock_get = zephyr_requests_get({"BLOCK-T1": "First case", "BLOCK-T2": "Second case"})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T1", "BLOCK-T2"])
        assert result == {"BLOCK-T1": "First case", "BLOCK-T2": "Second case"}

    def test_suffixed_key_queries_parent(self, adapter):
        mock_get = zephyr_requests_get({"BLOCK-T123": "Parent case"})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T123_a"])
        assert result == {"BLOCK-T123_a": "Parent case"}
        assert mock_get.call_args.args[0] == f"{SERVER}/testcases/BLOCK-T123"

    def test_suffix_variants_share_one_fetch(self, adapter):
        mock_get = zephyr_requests_get({"BLOCK-T123": "Parent case"})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T123_a", "BLOCK-T123_b", "BLOCK-T123"])
        assert result == {
            "BLOCK-T123_a": "Parent case",
            "BLOCK-T123_b": "Parent case",
            "BLOCK-T123": "Parent case",
        }
        assert mock_get.call_count == 1

    def test_unknown_key_yields_none(self, adapter):
        mock_get = zephyr_requests_get({"BLOCK-T1": "First case"})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T1", "BLOCK-T404"])
        assert result == {"BLOCK-T1": "First case", "BLOCK-T404": None}

    def test_non_404_http_error_propagates(self, adapter):
        response = Mock()
        response.status_code = 502
        response.raise_for_status.side_effect = requests.HTTPError("502 Bad Gateway", response=response)
        with patch("requests.get", return_value=response):
            with pytest.raises(requests.HTTPError):
                adapter.fetch_by_ids(["BLOCK-T1"])

    def test_bearer_auth_header(self, adapter):
        mock_get = zephyr_requests_get({"BLOCK-T1": "First case"})
        with patch("requests.get", mock_get):
            adapter.fetch_by_ids(["BLOCK-T1"])
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer test-token"


class TestCaching:
    def test_cache_keyed_by_parent(self, adapter, fake_redis):
        with patch("requests.get", zephyr_requests_get({"BLOCK-T123": "Parent case"})):
            adapter.fetch_by_ids(["BLOCK-T123_a"])
        assert "adapter:zephyr:BLOCK-T123" in fake_redis.keys()
        assert "adapter:zephyr:BLOCK-T123_a" not in fake_redis.keys()

    def test_new_suffix_hits_parent_cache(self, adapter):
        with patch("requests.get", zephyr_requests_get({"BLOCK-T123": "Parent case"})):
            adapter.fetch_by_ids(["BLOCK-T123_a"])
        mock_get = zephyr_requests_get({})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T123_b"])
        assert result == {"BLOCK-T123_b": "Parent case"}
        mock_get.assert_not_called()

    def test_only_missing_parents_are_fetched(self, adapter):
        with patch("requests.get", zephyr_requests_get({"BLOCK-T1": "First case"})):
            adapter.fetch_by_ids(["BLOCK-T1"])
        mock_get = zephyr_requests_get({"BLOCK-T2": "Second case"})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T1", "BLOCK-T2"])
        assert result == {"BLOCK-T1": "First case", "BLOCK-T2": "Second case"}
        assert mock_get.call_count == 1
        assert mock_get.call_args.args[0] == f"{SERVER}/testcases/BLOCK-T2"

    def test_none_result_is_cached(self, adapter):
        with patch("requests.get", zephyr_requests_get({})):
            adapter.fetch_by_ids(["BLOCK-T404"])
        mock_get = zephyr_requests_get({})
        with patch("requests.get", mock_get):
            result = adapter.fetch_by_ids(["BLOCK-T404"])
        assert result == {"BLOCK-T404": None}
        mock_get.assert_not_called()

    def test_entries_stored_with_mutable_ttl(self, adapter, fake_redis):
        with patch("requests.get", zephyr_requests_get({"BLOCK-T1": "First case"})):
            adapter.fetch_by_ids(["BLOCK-T1"])
        assert fake_redis.ttls["adapter:zephyr:BLOCK-T1"] == MUTABLE_TTL_REDIS
