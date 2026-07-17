from unittest.mock import Mock, patch

import pytest
import requests

from lsst.ts.logging_and_reporting.adapters.nightreport import NightReportCachedAdapter

SERVER = "https://test.example"


def make_report(day_obs, report_id="report-1"):
    return {
        "id": report_id,
        "day_obs": day_obs,
        "summary": "summary",
        "date_added": f"{day_obs}T22:00:00",
    }


def mock_response(payload):
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return NightReportCachedAdapter(fake_redis, server_url=SERVER)


class TestFetchFromSource:
    def test_partitions_reports_by_dayobs(self, adapter):
        payload = [make_report(20250101), make_report(20250102), make_report(20250102)]
        with patch("requests.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert len(result[20250101]) == 1
        assert len(result[20250102]) == 2

    def test_every_requested_dayobs_present_even_when_empty(self, adapter):
        with patch("requests.get", return_value=mock_response([])):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert result == {20250101: [], 20250102: []}

    def test_out_of_range_reports_dropped(self, adapter):
        payload = [make_report(20250105)]
        with patch("requests.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101])
        assert result == {20250101: []}

    def test_one_request_per_contiguous_run_with_exclusive_max(self, adapter):
        with patch("requests.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101, 20250102, 20250105])
        assert mock_get.call_count == 2
        ranges = [
            (call.kwargs["params"]["min_day_obs"], call.kwargs["params"]["max_day_obs"])
            for call in mock_get.call_args_list
        ]
        assert ranges == [(20250101, 20250103), (20250105, 20250106)]

    def test_request_url_and_fixed_params(self, adapter):
        with patch("requests.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101])
        assert mock_get.call_args.args[0] == f"{SERVER}/nightreport/reports"
        params = mock_get.call_args.kwargs["params"]
        assert params["is_human"] == "either"
        assert params["is_valid"] == "true"
        assert params["order_by"] == "-day_obs"

    def test_paginates_until_short_page(self, fake_redis, monkeypatch):
        monkeypatch.setenv("ACCESS_TOKEN", "test-token")
        adapter = NightReportCachedAdapter(fake_redis, server_url=SERVER, page_limit=2)
        pages = [
            [make_report(20250101, "r1"), make_report(20250101, "r2")],
            [make_report(20250101, "r3")],
        ]
        with patch("requests.get", side_effect=[mock_response(page) for page in pages]) as mock_get:
            result = adapter._fetch_from_source([20250101])
        assert len(result[20250101]) == 3
        offsets = [call.kwargs["params"]["offset"] for call in mock_get.call_args_list]
        assert offsets == [0, 2]

    def test_http_error_propagates(self, adapter):
        response = mock_response([])
        response.raise_for_status.side_effect = requests.HTTPError("502 Bad Gateway")
        with patch("requests.get", return_value=response):
            with pytest.raises(requests.HTTPError):
                adapter._fetch_from_source([20250101])

    def test_auth_header_uses_service_token(self, adapter):
        with patch("requests.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101])
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer test-token"


class TestTtl:
    def test_historical_dayobs_gets_long_ttl(self, adapter, fake_redis):
        payload = [make_report(20200101)]
        with patch("requests.get", return_value=mock_response(payload)):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:nightreport:20200101"] == adapter.LONG_TTL
