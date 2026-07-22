from unittest.mock import Mock

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.adapters.rubin_nights_obs_status import RubinNightsObsStatusAdapter
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS, TODAY_TTL_REDIS
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs


@pytest.fixture
def adapter(fake_redis):
    a = RubinNightsObsStatusAdapter(fake_redis)
    # Bypass get_clients / EFD credentials; select_time_series is mocked.
    a.__dict__["_efd_client"] = Mock()
    return a


def status_frame(events):
    """Build a tz-aware UTC time-indexed frame like the EFD returns."""
    index = pd.DatetimeIndex([pd.Timestamp(event["time"], tz="UTC") for event in events])
    data = {
        "status": [event["status"] for event in events],
        "note": [event.get("note", "") for event in events],
        "statusLabels": [event.get("statusLabels", "") for event in events],
    }
    return pd.DataFrame(data, index=index)


class TestFetch:
    def test_partitions_events_by_dayobs(self, adapter):
        frame = status_frame(
            [
                {"time": "2025-01-01T23:00:00", "status": 2},
                {"time": "2025-01-02T02:00:00", "status": 4},
                {"time": "2025-01-02T23:00:00", "status": 2},
            ]
        )
        adapter._efd_client.select_time_series.return_value = frame
        result = adapter.fetch(20250101, 20250102)
        assert {dayobs: len(rows) for dayobs, rows in result.items()} == {20250101: 2, 20250102: 1}

    def test_adds_time_ms_and_keeps_fields(self, adapter):
        frame = status_frame([{"time": "2025-01-01T23:00:00", "status": 2, "note": "n"}])
        adapter._efd_client.select_time_series.return_value = frame
        record = adapter.fetch(20250101, 20250101)[20250101][0]
        assert record["status"] == 2
        assert record["note"] == "n"
        assert record["time_ms"] == pd.Timestamp("2025-01-01T23:00:00Z").value // 1_000_000

    def test_empty_frame_yields_empty_lists(self, adapter):
        adapter._efd_client.select_time_series.return_value = pd.DataFrame([])
        result = adapter.fetch(20250101, 20250102)
        assert result == {20250101: [], 20250102: []}

    def test_one_query_per_contiguous_run(self, adapter):
        adapter._efd_client.select_time_series.return_value = pd.DataFrame([])
        adapter._fetch_from_source([20250101, 20250102, 20250105])
        assert adapter._efd_client.select_time_series.call_count == 2

    def test_events_outside_requested_days_dropped(self, adapter):
        frame = status_frame([{"time": "2025-01-09T23:00:00", "status": 2}])
        adapter._efd_client.select_time_series.return_value = frame
        result = adapter.fetch(20250101, 20250101)
        assert result == {20250101: []}


class TestTtl:
    def test_historic_ttl_for_past_dayobs(self, adapter, fake_redis):
        adapter._efd_client.select_time_series.return_value = pd.DataFrame([])
        adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:rubin_nights_obs_status:20200101"] == HISTORIC_TTL_REDIS

    def test_today_ttl_for_today(self, adapter, fake_redis):
        today = current_dayobs()
        adapter._efd_client.select_time_series.return_value = pd.DataFrame([])
        adapter.fetch(today, today)
        assert fake_redis.ttls[f"adapter:rubin_nights_obs_status:{today}"] == TODAY_TTL_REDIS
