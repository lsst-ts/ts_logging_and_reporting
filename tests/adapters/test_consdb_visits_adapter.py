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
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.adapters.consdb_visits import ConsdbVisitsAdapter

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
def visits_adapter(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return ConsdbVisitsAdapter(fake_redis, server_url=SERVER)


class TestVisitsAdapter:
    def test_queries_visit_tables_by_dayobs(self, visits_adapter):
        mock_post = consdb_post([])
        with patch("requests.Session.post", mock_post):
            visits_adapter._fetch_run("lsstcam", 20250101, 20250102)
        sql = sent_sql(mock_post)
        assert sql.startswith("SELECT v.*, q.*")
        assert "FROM cdb_lsstcam.visit1 v" in sql
        assert "LEFT JOIN cdb_lsstcam.visit1_quicklook q ON v.visit_id = q.visit_id" in sql
        assert "20250101 <= v.day_obs AND v.day_obs <= 20250102" in sql
        assert "exposure_efd" not in sql

    def test_partitions_records_by_dayobs(self, visits_adapter):
        records = [
            {"visit_id": 1, "day_obs": 20250101},
            {"visit_id": 2, "day_obs": 20250102},
        ]
        with patch("requests.Session.post", consdb_post(records)):
            result = visits_adapter.fetch("LSSTCam", 20250101, 20250102)
        assert {dayobs: len(rows) for dayobs, rows in result.items()} == {20250101: 1, 20250102: 1}

    def test_uses_own_cache_namespace(self, visits_adapter, fake_redis):
        with patch("requests.Session.post", consdb_post([])):
            visits_adapter.fetch("lsstcam", 20250101, 20250101)
        assert "adapter:consdb_visits:lsstcam:20250101" in fake_redis.keys()

    def test_unknown_instrument_raises_422(self, visits_adapter):
        with pytest.raises(HTTPException) as exc:
            visits_adapter.fetch("lsstComCam", 20250101, 20250101)
        assert exc.value.status_code == 422

    def test_malformed_dayobs_raises_422(self, visits_adapter):
        with pytest.raises(HTTPException) as exc:
            visits_adapter.fetch("lsstcam", 20259999, 20250101)
        assert exc.value.status_code == 422


class TestValidation:
    def test_validate_instrument_accepts_known_instrument(self, visits_adapter):
        assert visits_adapter._validate_instrument("LSSTCam") == "lsstcam"

    def test_validate_instrument_rejects_unknown_and_logs(self, visits_adapter, caplog):
        with caplog.at_level(logging.WARNING):
            with pytest.raises(HTTPException) as exc:
                visits_adapter._validate_instrument("lsstComCam")
        assert exc.value.status_code == 422
        assert "Rejected unknown instrument: 'lsstComCam'" in caplog.text

    def test_validate_dayobs_accepts_well_formed_dayobs(self, visits_adapter):
        visits_adapter._validate_dayobs(20250101)

    def test_validate_dayobs_rejects_malformed_and_logs(self, visits_adapter, caplog):
        with caplog.at_level(logging.WARNING):
            with pytest.raises(HTTPException) as exc:
                visits_adapter._validate_dayobs(20259999)
        assert exc.value.status_code == 422
        assert "Rejected malformed dayobs: 20259999" in caplog.text
