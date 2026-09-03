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

import logging
from unittest.mock import Mock, patch

import pytest
import requests
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.adapters.consdb_exposures import ConsdbExposuresAdapter
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS, TODAY_TTL_REDIS
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

SERVER = "https://consdb.test"


def consdb_post(records, columns=None):
    """Mock ``requests.post`` returning a ConsDB columns/data payload."""
    if columns is None:
        columns = list(records[0].keys()) if records else []
    data = [[record.get(column) for column in columns] for record in records]
    response = Mock()
    response.json.return_value = {"columns": columns, "data": data}
    response.raise_for_status.return_value = None
    return Mock(return_value=response)


def sent_sql(mock_post):
    return mock_post.call_args.kwargs["json"]["query"]


@pytest.fixture
def adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return ConsdbExposuresAdapter(fake_redis, server_url=SERVER)


class TestQueryBuilding:
    def test_efd_channels_folded_in_for_lsstcam(self, adapter):
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter._fetch_run("lsstcam", 20250101, 20250101)
        sql = sent_sql(mock_post)
        assert "SELECT e.*, q.*, f.mt_salindex112_temperature_0_mean" in sql
        assert "LEFT JOIN efd_lsstcam.exposure_efd f ON e.exposure_id = f.exposure_id" in sql
        assert "20250101 <= e.day_obs AND e.day_obs <= 20250101" in sql

    def test_no_efd_join_for_latiss(self, adapter):
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter._fetch_run("latiss", 20250101, 20250101)
        sql = sent_sql(mock_post)
        assert "exposure_efd" not in sql
        assert sql.startswith("SELECT e.*, q.*")

    def test_posts_to_consdb_query_endpoint(self, adapter):
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter._fetch_run("lsstcam", 20250101, 20250101)
        assert mock_post.call_args.args[0] == f"{SERVER}/consdb/query"

    @pytest.mark.parametrize(
        "deployment",
        [
            "https://summit-lsp.lsst.codes",
            "https://base-lsp.lsst.codes",
        ],
    )
    def test_efd_join_omitted_where_transform_unavailable(self, adapter, monkeypatch, deployment):
        monkeypatch.setenv("EXTERNAL_INSTANCE_URL", deployment)
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter._fetch_run("lsstcam", 20250101, 20250101)
        sql = sent_sql(mock_post)
        assert "exposure_efd" not in sql
        assert sql.startswith("SELECT e.*, q.*")

    def test_efd_join_kept_at_efd_available_deployment(self, adapter, monkeypatch):
        monkeypatch.setenv("EXTERNAL_INSTANCE_URL", "https://usdf-rsp.slac.stanford.edu")
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter._fetch_run("lsstcam", 20250101, 20250101)
        assert "LEFT JOIN efd_lsstcam.exposure_efd" in sent_sql(mock_post)


class TestRowShaping:
    def test_zips_columns_and_data_into_records(self, adapter):
        result = {"columns": ["exposure_id", "day_obs", "seq_num"], "data": [[1, 20250101, 5]]}
        assert adapter._rows_from_result(result) == [{"exposure_id": 1, "day_obs": 20250101, "seq_num": 5}]

    def test_duplicate_column_keeps_first_non_null(self, adapter):
        # exposure.day_obs valid, quicklook.day_obs null under the same name.
        result = {"columns": ["exposure_id", "day_obs", "day_obs"], "data": [[1, 20250101, None]]}
        assert adapter._rows_from_result(result) == [{"exposure_id": 1, "day_obs": 20250101}]

    def test_duplicate_column_null_does_not_clobber_valid(self, adapter):
        # first copy null, second copy valid -> keep the valid one.
        result = {"columns": ["exposure_id", "val", "val"], "data": [[1, None, 7]]}
        assert adapter._rows_from_result(result) == [{"exposure_id": 1, "val": 7}]


class TestFetch:
    def test_partitions_records_by_dayobs(self, adapter):
        records = [
            {"exposure_id": 1, "day_obs": 20250101, "seq_num": 2},
            {"exposure_id": 2, "day_obs": 20250101, "seq_num": 1},
            {"exposure_id": 3, "day_obs": 20250102, "seq_num": 1},
        ]
        with patch("requests.Session.post", consdb_post(records)):
            result = adapter.fetch("LSSTCam", 20250101, 20250102)
        assert {dayobs: len(rows) for dayobs, rows in result.items()} == {20250101: 2, 20250102: 1}

    def test_instrument_lower_cased_in_cache_key(self, adapter, fake_redis):
        with patch("requests.Session.post", consdb_post([])):
            adapter.fetch("LSSTCam", 20250101, 20250101)
        assert "adapter:consdb_exposures:lsstcam:20250101" in fake_redis.keys()

    def test_one_query_per_contiguous_run(self, adapter):
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter._fetch_from_source(["lsstcam:20250101", "lsstcam:20250102", "lsstcam:20250105"])
        assert mock_post.call_count == 2

    def test_auth_header_uses_service_token(self, adapter):
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            adapter.fetch("lsstcam", 20250101, 20250101)
        assert mock_post.call_args.kwargs["headers"]["Authorization"] == "Bearer test-token"

    def test_http_error_propagates(self, adapter):
        response = Mock()
        response.json.return_value = {"columns": [], "data": []}
        response.raise_for_status.side_effect = requests.HTTPError("500 Internal Server Error")
        with patch("requests.Session.post", Mock(return_value=response)):
            with pytest.raises(requests.HTTPError):
                adapter.fetch("lsstcam", 20250101, 20250101)


class TestValidation:
    def test_unknown_instrument_raises_422(self, adapter):
        with pytest.raises(HTTPException) as exc:
            adapter.fetch("lsstComCam", 20250101, 20250101)
        assert exc.value.status_code == 422

    def test_malformed_dayobs_raises_422(self, adapter):
        with pytest.raises(HTTPException) as exc:
            adapter.fetch("lsstcam", 20259999, 20250101)
        assert exc.value.status_code == 422

    def test_validate_instrument_accepts_known_instrument(self, adapter):
        assert adapter._validate_instrument("LSSTCam") == "lsstcam"

    def test_validate_instrument_rejects_unknown_and_logs(self, adapter, caplog):
        with caplog.at_level(logging.WARNING):
            with pytest.raises(HTTPException) as exc:
                adapter._validate_instrument("lsstComCam")
        assert exc.value.status_code == 422
        assert "Rejected unknown instrument: 'lsstComCam'" in caplog.text

    def test_validate_dayobs_accepts_well_formed_dayobs(self, adapter):
        adapter._validate_dayobs(20250101)

    def test_validate_dayobs_rejects_malformed_and_logs(self, adapter, caplog):
        with caplog.at_level(logging.WARNING):
            with pytest.raises(HTTPException) as exc:
                adapter._validate_dayobs(20259999)
        assert exc.value.status_code == 422
        assert "Rejected malformed dayobs: 20259999" in caplog.text


class TestTtl:
    def test_historic_ttl_for_past_dayobs(self, adapter, fake_redis):
        with patch("requests.Session.post", consdb_post([])):
            adapter.fetch("lsstcam", 20200101, 20200101)
        assert fake_redis.ttls["adapter:consdb_exposures:lsstcam:20200101"] == HISTORIC_TTL_REDIS

    def test_today_ttl_for_today(self, adapter, fake_redis):
        today = current_dayobs()
        with patch("requests.Session.post", consdb_post([])):
            adapter.fetch("lsstcam", today, today)
        assert fake_redis.ttls[f"adapter:consdb_exposures:lsstcam:{today}"] == TODAY_TTL_REDIS
