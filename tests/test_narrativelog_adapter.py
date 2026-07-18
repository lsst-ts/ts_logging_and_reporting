from unittest.mock import Mock, patch

import pytest
import requests

from lsst.ts.logging_and_reporting.adapters.narrativelog import NarrativelogCachedAdapter
from lsst.ts.logging_and_reporting.web_app.cache_ttl import MUTABLE_TTL_REDIS

SERVER = "https://test.example"


def make_message(date_begin, telescope="Simonyi", date_added=None, time_lost=0.0, time_lost_type=None):
    return {
        "id": f"msg-{date_begin}",
        "date_begin": date_begin,
        "date_added": date_added or date_begin,
        "components_json": {"name": telescope} if telescope else None,
        "message_text": "text",
        "time_lost": time_lost,
        "time_lost_type": time_lost_type,
    }


def mock_response(payload):
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return NarrativelogCachedAdapter(fake_redis, server_url=SERVER)


class TestFetchFromSource:
    def test_partitions_messages_by_date_begin_dayobs(self, adapter):
        payload = [
            make_message("2025-01-01T22:00:00"),
            make_message("2025-01-02T02:00:00"),
            make_message("2025-01-02T13:00:00"),
        ]
        with patch("requests.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert len(result[20250101]) == 2
        assert len(result[20250102]) == 1

    def test_every_requested_dayobs_present_even_when_empty(self, adapter):
        with patch("requests.get", return_value=mock_response([])):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert result == {20250101: [], 20250102: []}

    def test_out_of_range_messages_dropped(self, adapter):
        payload = [make_message("2025-01-05T22:00:00")]
        with patch("requests.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101])
        assert result == {20250101: []}

    def test_one_request_per_contiguous_run_with_noon_window(self, adapter):
        with patch("requests.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101, 20250102, 20250105])
        assert mock_get.call_count == 2
        windows = [
            (call.kwargs["params"]["min_date_begin"], call.kwargs["params"]["max_date_begin"])
            for call in mock_get.call_args_list
        ]
        assert windows == [
            ("2025-01-01T12:00:00", "2025-01-03T12:00:00"),
            ("2025-01-05T12:00:00", "2025-01-06T12:00:00"),
        ]

    def test_request_url_and_fixed_params(self, adapter):
        with patch("requests.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101])
        assert mock_get.call_args.args[0] == f"{SERVER}/narrativelog/messages"
        params = mock_get.call_args.kwargs["params"]
        assert params["is_human"] == "either"
        assert params["is_valid"] == "true"
        assert params["order_by"] == "-date_begin"
        assert "instrument" not in params

    def test_paginates_until_short_page(self, fake_redis, monkeypatch):
        monkeypatch.setenv("ACCESS_TOKEN", "test-token")
        adapter = NarrativelogCachedAdapter(fake_redis, server_url=SERVER, page_limit=2)
        pages = [
            [make_message("2025-01-01T20:00:00"), make_message("2025-01-01T21:00:00")],
            [make_message("2025-01-01T22:00:00")],
        ]
        with patch("requests.get", side_effect=[mock_response(page) for page in pages]) as mock_get:
            result = adapter._fetch_from_source([20250101])
        assert len(result[20250101]) == 3
        offsets = [call.kwargs["params"]["offset"] for call in mock_get.call_args_list]
        assert offsets == [0, 2]

    def test_record_cap_truncates_runaway_pagination(self, fake_redis, monkeypatch):
        monkeypatch.setenv("ACCESS_TOKEN", "test-token")
        adapter = NarrativelogCachedAdapter(fake_redis, server_url=SERVER, page_limit=2)
        adapter.MAX_RECORDS = 4
        full_page = [make_message("2025-01-01T20:00:00"), make_message("2025-01-01T21:00:00")]
        with patch("requests.get", return_value=mock_response(full_page)) as mock_get:
            result = adapter._fetch_from_source([20250101])
        assert len(result[20250101]) == 4
        assert mock_get.call_count == 2

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


class TestInstrumentDerivation:
    def fetched_instrument(self, adapter, message):
        with patch("requests.get", return_value=mock_response([message])):
            result = adapter._fetch_from_source([20250101])
        return result[20250101][0]["instrument"]

    def test_auxtel_maps_to_latiss(self, adapter):
        message = make_message("2025-01-01T22:00:00", telescope="AuxTel")
        assert self.fetched_instrument(adapter, message) == "LATISS"

    def test_simonyi_maps_to_lsstcam(self, adapter):
        message = make_message("2025-01-01T22:00:00", telescope="Simonyi", date_added="2025-02-01T00:00:00")
        assert self.fetched_instrument(adapter, message) == "LSSTCam"

    def test_maintel_before_lsstcam_era_maps_to_comcam(self, adapter):
        message = make_message("2025-01-01T22:00:00", telescope="MainTel", date_added="2025-01-19T00:00:00")
        assert self.fetched_instrument(adapter, message) == "LSSTComCam"

    def test_unknown_telescope_maps_to_none(self, adapter):
        message = make_message("2025-01-01T22:00:00", telescope="OtherScope")
        assert self.fetched_instrument(adapter, message) is None

    def test_missing_components_maps_to_none(self, adapter):
        message = make_message("2025-01-01T22:00:00", telescope=None)
        assert self.fetched_instrument(adapter, message) is None


class TestTtl:
    def test_mutable_ttl_for_historical_dayobs(self, adapter, fake_redis):
        payload = [make_message("2020-01-01T22:00:00")]
        with patch("requests.get", return_value=mock_response(payload)):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:narrativelog:20200101"] == MUTABLE_TTL_REDIS
