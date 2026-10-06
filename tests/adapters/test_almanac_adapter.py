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

import datetime as dt
import threading
from unittest.mock import Mock

import pytest

from lsst.ts.logging_and_reporting.adapters.almanac import AlmanacCachedAdapter
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

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


class TestSerialComputation:
    """A fragmented request computes its runs on the calling thread."""

    def test_parallel_runs_are_capped_at_one(self):
        # Pinned deliberately rather than left at the base class
        # default: these runs are local CPU work, so spreading them
        # over threads only contends for the GIL and takes CPU from
        # the threads serving other requests.
        assert AlmanacCachedAdapter.MAX_PARALLEL_RUNS == 1

    def test_split_runs_are_never_fanned_out(self, adapter):
        threads = []

        def compute(dayobs):
            threads.append(threading.current_thread())
            return {"dayobs": dayobs}

        adapter._compute_night = Mock(side_effect=compute)
        # Caching the middle day splits the next request's misses into
        # two runs — the shape an adapter with a higher cap fans out.
        adapter.fetch(20250102, 20250102)
        adapter.fetch(20250101, 20250103)

        assert adapter._compute_night.call_count == 3
        assert set(threads) == {threading.current_thread()}


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
