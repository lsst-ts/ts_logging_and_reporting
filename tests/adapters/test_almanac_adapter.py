import datetime as dt
from unittest.mock import Mock

import pytest

from lsst.ts.logging_and_reporting.adapters.almanac import AlmanacCachedAdapter
from lsst.ts.logging_and_reporting.utils import current_dayobs
from lsst.ts.logging_and_reporting.web_app.cache_ttl import HISTORIC_TTL_REDIS

TODAY = current_dayobs()


@pytest.fixture
def adapter(fake_redis):
    return AlmanacCachedAdapter(fake_redis)


class TestCaching:
    def test_computes_each_requested_dayobs(self, adapter):
        adapter._compute_night = Mock(side_effect=lambda dayobs: {"dayobs": dayobs})
        result = adapter._fetch_from_source([20250101, 20250102])
        assert result == {20250101: {"dayobs": 20250101}, 20250102: {"dayobs": 20250102}}

    def test_second_fetch_serves_from_cache(self, adapter):
        adapter._compute_night = Mock(side_effect=lambda dayobs: {"dayobs": dayobs})
        adapter.fetch(20250101, 20250101)
        adapter.fetch(20250101, 20250101)
        assert adapter._compute_night.call_count == 1

    def test_historic_ttl_even_for_today(self, adapter, fake_redis):
        adapter._compute_night = Mock(side_effect=lambda dayobs: {"dayobs": dayobs})
        adapter.fetch(TODAY, TODAY)
        assert fake_redis.ttls[f"adapter:almanac:{TODAY}"] == HISTORIC_TTL_REDIS


class TestComputeNight:
    def test_computed_record_is_consistent_and_roundtrips(self, adapter):
        first = adapter.fetch(20250731, 20250731)
        record = first[20250731]

        assert record["dayobs"] == 20250731
        evening = dt.datetime.fromisoformat(record["twilight_evening_12deg"])
        morning = dt.datetime.fromisoformat(record["twilight_morning_12deg"])
        # Morning-boundary labeling: the record describes the night
        # from the evening of the previous date to this date's morning.
        assert evening.date() == dt.date(2025, 7, 30)
        assert morning.date() == dt.date(2025, 7, 31)
        assert record["night_hours"] == pytest.approx((morning - evening).total_seconds() / 3600, abs=0.02)

        evening_18 = dt.datetime.fromisoformat(record["twilight_evening_18deg"])
        morning_18 = dt.datetime.fromisoformat(record["twilight_morning_18deg"])
        assert evening < evening_18 < morning_18 < morning

        assert record["moon_illumination"].endswith("%")

        # Cached copy JSON-roundtrips to the same record
        assert adapter.fetch(20250731, 20250731) == first
