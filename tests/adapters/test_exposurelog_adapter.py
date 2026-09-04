# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

from unittest.mock import Mock, patch

import pytest
import requests

from lsst.ts.logging_and_reporting.adapters.exposurelog import ExposurelogCachedAdapter
from lsst.ts.logging_and_reporting.cache_ttl import MUTABLE_TTL_REDIS

SERVER = "https://test.example"


def make_message(day_obs, instrument="LSSTCam", flag="junk", obs_id="obs-1"):
    return {
        "obs_id": obs_id,
        "instrument": instrument,
        "day_obs": day_obs,
        "exposure_flag": flag,
        "date_added": f"{day_obs}T00:00:00",
    }


def mock_response(payload):
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return ExposurelogCachedAdapter(fake_redis, server_url=SERVER)


class TestFetchFromSource:
    def test_partitions_messages_by_dayobs(self, adapter):
        payload = [make_message(20250101), make_message(20250102), make_message(20250102)]
        with patch("requests.Session.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert len(result[20250101]) == 1
        assert len(result[20250102]) == 2

    def test_every_requested_dayobs_present_even_when_empty(self, adapter):
        with patch("requests.Session.get", return_value=mock_response([])):
            result = adapter._fetch_from_source([20250101, 20250102])
        assert result == {20250101: [], 20250102: []}

    def test_out_of_range_messages_dropped(self, adapter):
        payload = [make_message(20250105)]
        with patch("requests.Session.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101])
        assert result == {20250101: []}

    def test_one_request_per_contiguous_run_with_exclusive_max(self, adapter):
        with patch("requests.Session.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101, 20250102, 20250105])
        assert mock_get.call_count == 2
        ranges = [
            (call.kwargs["params"]["min_day_obs"], call.kwargs["params"]["max_day_obs"])
            for call in mock_get.call_args_list
        ]
        assert ranges == [(20250101, 20250103), (20250105, 20250106)]

    def test_request_url_and_fixed_params(self, adapter):
        with patch("requests.Session.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101])
        assert mock_get.call_args.args[0] == f"{SERVER}/exposurelog/messages"
        params = mock_get.call_args.kwargs["params"]
        assert params["is_human"] == "true"
        assert params["order_by"] == "-date_added"
        assert "instrument" not in params

    def test_none_flag_mapped_to_unknown(self, adapter):
        payload = [make_message(20250101, flag="none")]
        with patch("requests.Session.get", return_value=mock_response(payload)):
            result = adapter._fetch_from_source([20250101])
        assert result[20250101][0]["exposure_flag"] == "unknown"

    def test_http_error_propagates(self, adapter):
        response = mock_response([])
        response.raise_for_status.side_effect = requests.HTTPError("502 Bad Gateway")
        with patch("requests.Session.get", return_value=response):
            with pytest.raises(requests.HTTPError):
                adapter._fetch_from_source([20250101])

    def test_auth_header_uses_service_token(self, adapter):
        with patch("requests.Session.get", return_value=mock_response([])) as mock_get:
            adapter._fetch_from_source([20250101])
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer test-token"


class TestTtl:
    def test_mutable_ttl_for_historical_dayobs(self, adapter, fake_redis):
        payload = [make_message(20200101)]
        with patch("requests.Session.get", return_value=mock_response(payload)):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:exposurelog:20200101"] == MUTABLE_TTL_REDIS
