from unittest.mock import patch

import pytest
from rubin_sim.sim_archive import NoMatchingSimulationsFoundError

from lsst.ts.logging_and_reporting.adapters.expected_exposures import ExpectedExposuresCachedAdapter
from lsst.ts.logging_and_reporting.utils import current_dayobs
from lsst.ts.logging_and_reporting.web_app.cache_ttl import MUTABLE_TTL_REDIS, TODAY_TTL_REDIS

ADAPTER = "lsst.ts.logging_and_reporting.adapters.expected_exposures"


@pytest.fixture
def adapter(fake_redis):
    return ExpectedExposuresCachedAdapter(fake_redis)


class TestFetch:
    def test_caches_nominal_visits_per_dayobs(self, adapter):
        def stats(day_obs, max_simulation_age):
            return {"nominal_visits": {20250101: 100, 20250102: 200}[day_obs]}

        with patch(f"{ADAPTER}.fetch_sim_stats_for_night", side_effect=stats):
            result = adapter.fetch(20250101, 20250102)
        assert result == {20250101: 100, 20250102: 200}

    def test_end_is_inclusive(self, adapter):
        queried = []

        def stats(day_obs, max_simulation_age):
            queried.append(day_obs)
            return {"nominal_visits": 1}

        with patch(f"{ADAPTER}.fetch_sim_stats_for_night", side_effect=stats):
            adapter.fetch(20250101, 20250103)
        assert sorted(queried) == [20250101, 20250102, 20250103]

    def test_missing_nominal_visits_defaults_to_zero(self, adapter):
        with patch(f"{ADAPTER}.fetch_sim_stats_for_night", return_value={}):
            result = adapter.fetch(20250101, 20250101)
        assert result == {20250101: 0}

    def test_max_simulation_age_of_60_days(self, adapter):
        with patch(f"{ADAPTER}.fetch_sim_stats_for_night", return_value={"nominal_visits": 5}) as mock:
            adapter.fetch(20250101, 20250101)
        assert mock.call_args.kwargs["max_simulation_age"] == 60

    def test_no_matching_simulation_propagates(self, adapter):
        with patch(
            f"{ADAPTER}.fetch_sim_stats_for_night",
            side_effect=NoMatchingSimulationsFoundError("no sim"),
        ):
            with pytest.raises(NoMatchingSimulationsFoundError):
                adapter.fetch(20250101, 20250101)


class TestTtl:
    def test_mutable_ttl_for_past_dayobs(self, adapter, fake_redis):
        with patch(f"{ADAPTER}.fetch_sim_stats_for_night", return_value={"nominal_visits": 1}):
            adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:expected_exposures:20200101"] == MUTABLE_TTL_REDIS

    def test_today_ttl_for_today(self, adapter, fake_redis):
        today = current_dayobs()
        with patch(f"{ADAPTER}.fetch_sim_stats_for_night", return_value={"nominal_visits": 1}):
            adapter.fetch(today, today)
        assert fake_redis.ttls[f"adapter:expected_exposures:{today}"] == TODAY_TTL_REDIS
