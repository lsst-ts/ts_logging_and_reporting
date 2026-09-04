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

from unittest.mock import patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.adapters.rubin_nights_dome import RubinNightsDomeAdapter
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS, TODAY_TTL_REDIS
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

ADAPTER = "lsst.ts.logging_and_reporting.adapters.rubin_nights_dome"


@pytest.fixture
def adapter(fake_redis):
    a = RubinNightsDomeAdapter(fake_redis)
    # Bypass get_clients / EFD credentials; the upstream call is mocked.
    a.__dict__["_efd_client"] = object()
    return a


def dome_frame(rows):
    return pd.DataFrame(rows)


class TestFetch:
    def test_partitions_records_by_day_obs(self, adapter):
        frame = dome_frame(
            [
                {"open_time": "2025-01-01T23:00:00", "dome_hours": 10.0, "day_obs": 20250101},
                {"open_time": "2025-01-02T01:00:00", "dome_hours": 1.0, "day_obs": 20250101},
                {"open_time": "2025-01-02T23:00:00", "dome_hours": 2.0, "day_obs": 20250102},
            ]
        )
        with patch(f"{ADAPTER}.get_dome_open_close", return_value=frame):
            result = adapter.fetch(20250101, 20250102)
        assert {dayobs: len(rows) for dayobs, rows in result.items()} == {20250101: 2, 20250102: 1}
        assert result[20250102][0]["dome_hours"] == 2.0

    def test_empty_frame_yields_empty_lists(self, adapter):
        with patch(f"{ADAPTER}.get_dome_open_close", return_value=pd.DataFrame([])):
            result = adapter.fetch(20250101, 20250102)
        assert result == {20250101: [], 20250102: []}

    def test_one_query_per_contiguous_run(self, adapter):
        with patch(f"{ADAPTER}.get_dome_open_close", return_value=pd.DataFrame([])) as mock:
            adapter._fetch_from_source([20250101, 20250102, 20250105])
        assert mock.call_count == 2

    def test_records_outside_requested_days_are_dropped(self, adapter):
        frame = dome_frame([{"dome_hours": 1.0, "day_obs": 20250109}])
        with patch(f"{ADAPTER}.get_dome_open_close", return_value=frame):
            result = adapter.fetch(20250101, 20250101)
        assert result == {20250101: []}


class TestTtl:
    def test_historic_ttl_for_past_dayobs(self, adapter, fake_redis):
        with patch(f"{ADAPTER}.get_dome_open_close", return_value=pd.DataFrame([])):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:rubin_nights_dome:20200101"] == HISTORIC_TTL_REDIS

    def test_today_ttl_for_today(self, adapter, fake_redis):
        today = current_dayobs()
        with patch(f"{ADAPTER}.get_dome_open_close", return_value=pd.DataFrame([])):
            adapter.fetch(today, today)
        assert fake_redis.ttls[f"adapter:rubin_nights_dome:{today}"] == TODAY_TTL_REDIS
