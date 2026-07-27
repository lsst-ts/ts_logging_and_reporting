from unittest.mock import patch

import numpy as np
import pytest

from lsst.ts.logging_and_reporting.adapters.visit_overhead import (
    MAX_SCATTER,
    VisitOverheadAdapter,
)
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS, TODAY_TTL_REDIS
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

ADAPTER = "lsst.ts.logging_and_reporting.adapters.visit_overhead"


class StubExposuresAdapter:
    """Stands in for the composed ConsDB exposures adapter."""

    def __init__(self, per_day):
        self.per_day = per_day
        self.calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.calls.append((instrument, start_dayobs, end_dayobs))
        return {d: self.per_day.get(d, []) for d in range(start_dayobs, end_dayobs + 1)}


@pytest.fixture
def make_adapter(fake_redis):
    def _make(per_day):
        adapter = VisitOverheadAdapter(fake_redis, StubExposuresAdapter(per_day))
        # Bypass get_clients / EFD credentials; the slew query is patched.
        adapter.__dict__["_efd_client"] = object()
        return adapter

    return _make


def exposure(day_obs, seq_num, band="r", visit_gap=7200.0, can_see_sky=True):
    return {
        "day_obs": day_obs,
        "seq_num": seq_num,
        "obs_start": f"2025-01-01T0{seq_num}:00:00",
        "s_ra": 10.0,
        "s_dec": -30.0,
        "sky_rotation": 0.0,
        "obs_start_mjd": 60000.0 + seq_num,
        "band": band,
        "can_see_sky": can_see_sky,
        "visit_gap": visit_gap,
    }


def stub_augment(exposures_df, instrument, skip_rs_columns=True):
    # augment_visits passes the visit columns through unchanged here.
    return exposures_df.copy()


def stub_slew(slew_values):
    """add_model_slew_times replacement that sets slew_model per row."""

    def _slew(visits, efd, model_settle, dome_crawl=False):
        out = visits.copy()
        out["slew_model"] = slew_values[: len(out)]
        return out, None

    return _slew


class TestFetch:
    def test_partitions_overhead_rows_by_dayobs(self, make_adapter):
        adapter = make_adapter(
            {20250101: [exposure(20250101, 1), exposure(20250101, 2)], 20250102: [exposure(20250102, 1)]}
        )
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([10.0, 10.0, 10.0])),
        ):
            result = adapter.fetch("lsstcam", 20250101, 20250102)
        assert {d: len(rows) for d, rows in result.items()} == {20250101: 2, 20250102: 1}
        # Only the reduction columns are cached.
        assert set(result[20250102][0]) == {
            "day_obs",
            "obs_start",
            "can_see_sky",
            "band",
            "overhead",
            "visit_gap",
        }

    def test_overhead_is_capped_slew_against_visit_gap(self, make_adapter):
        # slew + MAX_SCATTER = 10 + 120 = 130 < visit_gap 7200 -> overhead 130.
        adapter = make_adapter({20250101: [exposure(20250101, 1, visit_gap=7200.0)]})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([10.0])),
        ):
            record = adapter.fetch("lsstcam", 20250101, 20250101)[20250101][0]
        assert record["overhead"] == pytest.approx(10.0 + MAX_SCATTER)

    def test_overhead_taken_from_visit_gap_when_smaller(self, make_adapter):
        # visit_gap 50 < slew + MAX_SCATTER (130) -> overhead 50.
        adapter = make_adapter({20250101: [exposure(20250101, 1, visit_gap=50.0)]})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([10.0])),
        ):
            record = adapter.fetch("lsstcam", 20250101, 20250101)[20250101][0]
        assert record["overhead"] == pytest.approx(50.0)

    def test_nan_slew_treated_as_zero(self, make_adapter):
        # slew NaN -> 0 + MAX_SCATTER = 120, capped against visit_gap 7200.
        adapter = make_adapter({20250101: [exposure(20250101, 1, visit_gap=7200.0)]})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([np.nan])),
        ):
            record = adapter.fetch("lsstcam", 20250101, 20250101)[20250101][0]
        assert record["overhead"] == pytest.approx(MAX_SCATTER)

    def test_empty_exposures_yields_empty_lists(self, make_adapter):
        adapter = make_adapter({})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([])),
        ):
            result = adapter.fetch("lsstcam", 20250101, 20250102)
        assert result == {20250101: [], 20250102: []}

    def test_reads_exposures_from_the_composed_adapter(self, make_adapter):
        adapter = make_adapter({20250101: [exposure(20250101, 1)]})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([10.0])),
        ):
            adapter.fetch("lsstcam", 20250101, 20250101)
        assert adapter._exposures_adapter.calls == [("lsstcam", 20250101, 20250101)]


class TestTtl:
    def test_historic_ttl_for_past_dayobs(self, make_adapter, fake_redis):
        adapter = make_adapter({})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([])),
        ):
            adapter.fetch("lsstcam", 20200101, 20200101)
        assert fake_redis.ttls["adapter:visit_overhead:lsstcam:20200101"] == HISTORIC_TTL_REDIS

    def test_today_ttl_for_today(self, make_adapter, fake_redis):
        today = current_dayobs()
        adapter = make_adapter({})
        with (
            patch(f"{ADAPTER}.rn_aug.augment_visits", side_effect=stub_augment),
            patch(f"{ADAPTER}.rn_sch.add_model_slew_times", side_effect=stub_slew([])),
        ):
            adapter.fetch("lsstcam", today, today)
        assert fake_redis.ttls[f"adapter:visit_overhead:lsstcam:{today}"] == TODAY_TTL_REDIS
