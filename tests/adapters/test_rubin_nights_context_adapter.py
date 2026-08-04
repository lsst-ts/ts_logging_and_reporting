import datetime as dt
from unittest.mock import patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.adapters.rubin_nights_context import (
    CONTEXT_FEED_COLS,
    RUN_END_MARGIN,
    RubinNightsContextAdapter,
)
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS, TODAY_TTL_REDIS
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

ADAPTER = "lsst.ts.logging_and_reporting.adapters.rubin_nights_context"


@pytest.fixture
def adapter(fake_redis):
    a = RubinNightsContextAdapter(fake_redis)
    # Bypass get_clients / credentials; the upstream call is mocked.
    a.__dict__["_clients"] = {}
    return a


def context_frame(rows):
    """Frame shaped like get_consolidated_messages' first return value."""
    data = {col: [row.get(col) for row in rows] for col in CONTEXT_FEED_COLS}
    data["time"] = [pd.Timestamp(row["time"], tz="UTC") for row in rows]
    return pd.DataFrame(data)


def consolidated(rows):
    return (context_frame(rows), CONTEXT_FEED_COLS)


class TestFetch:
    def test_partitions_messages_by_dayobs(self, adapter):
        frame = consolidated(
            [
                {"time": "2025-01-01T23:00:00", "name": "a"},
                {"time": "2025-01-02T02:00:00", "name": "b"},
                {"time": "2025-01-02T23:00:00", "name": "c"},
            ]
        )
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=frame):
            result = adapter.fetch(20250101, 20250102)
        assert {dayobs: len(rows) for dayobs, rows in result.items()} == {20250101: 2, 20250102: 1}
        assert result[20250102][0]["name"] == "c"

    def test_empty_frame_yields_empty_lists(self, adapter):
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(pd.DataFrame([]), [])):
            result = adapter.fetch(20250101, 20250102)
        assert result == {20250101: [], 20250102: []}

    def test_one_query_per_contiguous_run(self, adapter):
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(pd.DataFrame([]), [])) as mock:
            adapter._fetch_from_source([20250101, 20250102, 20250105])
        assert mock.call_count == 2

    def test_query_runs_past_the_final_dayobs_boundary(self, adapter):
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(pd.DataFrame([]), [])) as mock:
            adapter.fetch(20250101, 20250102)
        t_start, t_end = mock.call_args.args[:2]
        assert t_start.datetime == dt.datetime(2025, 1, 1, 12)
        assert t_end.datetime == dt.datetime(2025, 1, 3, 12) + RUN_END_MARGIN

    def test_margin_messages_land_in_their_own_dayobs(self, adapter):
        # Both are published after the boundary; only the first belongs
        # to the requested dayobs.
        frame = consolidated(
            [
                {"time": "2025-01-02T11:59:00", "name": "before_noon"},
                {"time": "2025-01-02T13:00:00", "name": "after_noon"},
            ]
        )
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=frame):
            result = adapter.fetch(20250101, 20250101)
        assert [record["name"] for record in result[20250101]] == ["before_noon"]

    def test_messages_outside_requested_days_dropped(self, adapter):
        frame = consolidated([{"time": "2025-01-09T23:00:00", "name": "a"}])
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=frame):
            result = adapter.fetch(20250101, 20250101)
        assert result == {20250101: []}

    def test_timestamps_serialized_to_iso(self, adapter):
        frame = consolidated([{"time": "2025-01-01T23:00:00", "name": "a"}])
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=frame):
            record = adapter.fetch(20250101, 20250101)[20250101][0]
        assert record["time"] == "2025-01-01T23:00:00+00:00"

    def test_non_display_columns_dropped(self, adapter):
        frame = pd.DataFrame(
            {
                "time": [pd.Timestamp("2025-01-01T23:00:00", tz="UTC")],
                "name": ["a"],
                "internal_only": ["secret"],
            }
        )
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(frame, CONTEXT_FEED_COLS)):
            record = adapter.fetch(20250101, 20250101)[20250101][0]
        assert "internal_only" not in record

    def test_special_floats_kept_as_tokens(self, adapter):
        frame = pd.DataFrame(
            {
                "time": [pd.Timestamp("2025-01-01T23:00:00", tz="UTC")],
                "name": ["a"],
                "script_salIndex": [float("nan")],
            }
        )
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(frame, CONTEXT_FEED_COLS)):
            record = adapter.fetch(20250101, 20250101)[20250101][0]
        assert record["script_salIndex"] == "NaN"


class TestTtl:
    def test_historic_ttl_for_past_dayobs(self, adapter, fake_redis):
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(pd.DataFrame([]), [])):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:rubin_nights_context:20200101"] == HISTORIC_TTL_REDIS

    def test_today_ttl_for_today(self, adapter, fake_redis):
        today = current_dayobs()
        with patch(f"{ADAPTER}.get_consolidated_messages", return_value=(pd.DataFrame([]), [])):
            adapter.fetch(today, today)
        assert fake_redis.ttls[f"adapter:rubin_nights_context:{today}"] == TODAY_TTL_REDIS
