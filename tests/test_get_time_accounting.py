# tests/test_time_accounting.py
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.services.exposures import (
    _compute_filter_changed,
    _obs_start_tai_to_utc_ms,
    _sum_on_sky_within_twilight,
    _twilight_windows_by_dayobs,
)
from lsst.ts.logging_and_reporting.services.rubin_nights_service import get_time_accounting

MODULE = "lsst.ts.logging_and_reporting.services.rubin_nights_service"
TWILIGHT_SUM_KEYS = (
    "sum_overhead_with_filter_change",
    "sum_overhead_without_filter_change",
    "sum_visit_gap_with_filter_change",
    "sum_visit_gap_without_filter_change",
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

# A single night: dayobs 20260409, twilights in UTC (naive strings,
# as get_almanac returns them).
ALMANAC_INFO = [
    {
        "dayobs": 20260410,  # labeled with morning twilight date
        "night_hours": 11.0,
        "twilight_evening_12deg": "2026-04-09 23:00:00",
        "twilight_morning_12deg": "2026-04-10 10:00:00",
    }
]

# TAI is UTC + 37s. obs_start values below are TAI ISO strings.
TAI_OFFSET_MS = 37_000  # 37 seconds in milliseconds

EVENING_12DEG_UTC_MS = int(pd.Timestamp("2026-04-09 23:00:00", tz="UTC").timestamp() * 1000)
MORNING_12DEG_UTC_MS = int(pd.Timestamp("2026-04-10 10:00:00", tz="UTC").timestamp() * 1000)

# obs_start in TAI that lands inside the twilight window when converted to UTC
OBS_START_IN_WINDOW_TAI = "2026-04-10T01:00:37.000000"  # UTC 01:00:00 → inside
OBS_START_BEFORE_WINDOW_TAI = "2026-04-09T22:59:37.000000"  # UTC 22:59:00 → before evening
OBS_START_AFTER_WINDOW_TAI = "2026-04-10T10:00:38.000000"  # UTC 10:00:01 → after morning


def make_visit(**kwargs) -> dict:
    """Minimal visit row with sensible defaults."""
    defaults = {
        "obs_start": OBS_START_IN_WINDOW_TAI,
        "day_obs": 20260409,
        "can_see_sky": True,
        "band": "r",
        "physical_filter": "r_57",
        "overhead": 3600.0,  # 1 hour in seconds
        "visit_gap": 7200.0,  # 2 hours in seconds
    }
    return {**defaults, **kwargs}


def make_visits_df(*rows) -> pd.DataFrame:
    if not rows:
        rows = ({},)
    return pd.DataFrame([make_visit(**r) for r in rows])


# ---------------------------------------------------------------------------
# _twilight_windows_by_dayobs
# ---------------------------------------------------------------------------


class TestTwilightWindowsByDayobs:
    def test_maps_to_visit_dayobs_not_almanac_dayobs(self):
        windows = _twilight_windows_by_dayobs(ALMANAC_INFO)
        # Almanac labels by morning twilight date (20260410),
        # but visits on that night carry day_obs 20260409.
        assert 20260409 in windows
        assert 20260410 not in windows

    def test_window_start_is_evening_twilight_ms(self):
        windows = _twilight_windows_by_dayobs(ALMANAC_INFO)
        start_ms, _ = windows[20260409]
        assert start_ms == EVENING_12DEG_UTC_MS

    def test_window_end_is_morning_twilight_ms(self):
        windows = _twilight_windows_by_dayobs(ALMANAC_INFO)
        _, end_ms = windows[20260409]
        assert end_ms == MORNING_12DEG_UTC_MS

    def test_multiple_nights(self):
        almanac = ALMANAC_INFO + [
            {
                "dayobs": 20260411,
                "night_hours": 11.0,
                "twilight_evening_12deg": "2026-04-10 23:00:00",
                "twilight_morning_12deg": "2026-04-11 10:00:00",
            }
        ]
        windows = _twilight_windows_by_dayobs(almanac)
        assert set(windows.keys()) == {20260409, 20260410}


# ---------------------------------------------------------------------------
# _obs_start_tai_to_utc_ms
# ---------------------------------------------------------------------------


class TestObsStartTaiToUtcMs:
    def test_tai_offset_applied(self):
        # TAI is 37s ahead of UTC; converting to UTC should subtract 37s.
        tai_series = pd.Series(["2026-04-10T01:00:37.000000"])
        result = _obs_start_tai_to_utc_ms(tai_series)
        expected_ms = int(pd.Timestamp("2026-04-10 01:00:00", tz="UTC").timestamp() * 1000)
        assert result.iloc[0] == pytest.approx(expected_ms, abs=1000)

    def test_null_values_produce_nan(self):
        tai_series = pd.Series([None, "2026-04-10T01:00:37.000000"])
        result = _obs_start_tai_to_utc_ms(tai_series)
        assert pd.isna(result.iloc[0])
        assert pd.notna(result.iloc[1])

    def test_all_null_returns_all_nan(self):
        tai_series = pd.Series([None, None])
        result = _obs_start_tai_to_utc_ms(tai_series)
        assert result.isna().all()

    def test_empty_series(self):
        result = _obs_start_tai_to_utc_ms(pd.Series([], dtype=object))
        assert len(result) == 0


# ---------------------------------------------------------------------------
# _compute_filter_changed
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# _compute_filter_changed
# (expects visits_sorted -- already ordered by obs_start)
# ---------------------------------------------------------------------------


class TestComputeFilterChanged:
    def test_first_visit_is_never_a_change(self):
        df = make_visits_df({"band": "r", "physical_filter": "r_57"})
        result = _compute_filter_changed(df)
        assert not result.iloc[0]

    def test_same_band_consecutive_visits_no_change(self):
        df = make_visits_df(
            {"obs_start": "2026-04-10T01:00:00", "band": "r", "physical_filter": "r_57"},
            {"obs_start": "2026-04-10T01:01:00", "band": "r", "physical_filter": "r_57"},
        )
        result = _compute_filter_changed(df)
        assert not result.iloc[1]

    def test_band_change_is_flagged(self):
        df = make_visits_df(
            {"obs_start": "2026-04-10T01:00:00", "band": "r", "physical_filter": "r_57"},
            {"obs_start": "2026-04-10T01:01:00", "band": "g", "physical_filter": "g_6"},
        )
        result = _compute_filter_changed(df)
        assert result.iloc[1]

    def test_physical_filter_change_with_same_band_not_flagged(self):
        # _compute_filter_changed only checks band, not physical_filter.
        # Two different physical filters in the same band are not a change.
        df = make_visits_df(
            {"obs_start": "2026-04-10T01:00:00", "band": "r", "physical_filter": "r_57"},
            {"obs_start": "2026-04-10T01:01:00", "band": "r", "physical_filter": "r_58"},
        )
        result = _compute_filter_changed(df)
        assert not result.iloc[1]

    def test_expects_pre_sorted_input(self):
        # _compute_filter_changed does not sort -- it uses shift(1) on
        # whatever order the DataFrame arrives in. The sort responsibility
        # lives in _sum_on_sky_within_twilight. Passing unsorted input
        # produces wrong results, which is intentional by design.
        df = make_visits_df(
            {"obs_start": "2026-04-10T01:01:00", "band": "g", "physical_filter": "g_6"},
            {"obs_start": "2026-04-10T01:00:00", "band": "r", "physical_filter": "r_57"},
        )
        result = _compute_filter_changed(df)
        # With unsorted input, row 0 (g) is first -- no change.
        # Row 1 (r after g) -- flagged as change.
        assert not result.iloc[0]
        assert result.iloc[1]

    def test_nan_band_to_non_nan_counts_as_change(self):
        df = make_visits_df(
            {"obs_start": "2026-04-10T01:00:00", "band": None, "physical_filter": "r_57"},
            {"obs_start": "2026-04-10T01:01:00", "band": "r", "physical_filter": "r_57"},
        )
        result = _compute_filter_changed(df)
        assert result.iloc[1]

    def test_nan_to_nan_does_not_count_as_change(self):
        df = make_visits_df(
            {"obs_start": "2026-04-10T01:00:00", "band": None, "physical_filter": None},
            {"obs_start": "2026-04-10T01:01:00", "band": None, "physical_filter": None},
        )
        result = _compute_filter_changed(df)
        assert not result.iloc[1]

    def test_single_visit_returns_false(self):
        df = make_visits_df({})
        result = _compute_filter_changed(df)
        assert len(result) == 1
        assert not result.iloc[0]


# ---------------------------------------------------------------------------
# _sum_on_sky_within_twilight
# (owns the sort -- this is where time-order tests belong)
# ---------------------------------------------------------------------------


class TestSumOnSkyWithinTwilight:
    def test_returns_all_four_keys(self):
        df = make_visits_df()
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert set(result.keys()) == {
            "sum_overhead_with_filter_change",
            "sum_overhead_without_filter_change",
            "sum_visit_gap_with_filter_change",
            "sum_visit_gap_without_filter_change",
        }

    def test_all_values_are_floats(self):
        df = make_visits_df()
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert all(isinstance(v, float) for v in result.values())

    def test_values_rounded_to_two_decimal_places(self):
        # 3601s / 3600 = 1.000277... → rounds to 1.0
        df = make_visits_df({"overhead": 3601.0, "visit_gap": 3601.0})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        for v in result.values():
            assert v == round(v, 2)

    def test_visit_before_evening_twilight_excluded(self):
        df = make_visits_df({"obs_start": OBS_START_BEFORE_WINDOW_TAI, "can_see_sky": True})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert all(v == 0.0 for v in result.values())

    def test_visit_after_morning_twilight_excluded(self):
        df = make_visits_df({"obs_start": OBS_START_AFTER_WINDOW_TAI, "can_see_sky": True})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert all(v == 0.0 for v in result.values())

    def test_can_see_sky_false_excluded(self):
        df = make_visits_df({"can_see_sky": False})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert all(v == 0.0 for v in result.values())

    def test_sums_converted_to_hours(self):
        df = make_visits_df({"overhead": 3600.0, "visit_gap": 7200.0})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert result["sum_overhead_without_filter_change"] == pytest.approx(1.0)
        assert result["sum_visit_gap_without_filter_change"] == pytest.approx(2.0)

    def test_sort_is_applied_before_filter_change_detection(self):
        # DataFrame arrives in reverse time order. Without an internal sort,
        # the second visit (r→g) would be compared against g, not r, and
        # would be wrongly flagged as no-change. The sort in
        # _sum_on_sky_within_twilight must fire before _compute_filter_changed.
        df = make_visits_df(
            {
                "obs_start": "2026-04-10T02:00:37.000",
                "band": "g",
                "physical_filter": "g_6",
                "overhead": 1800.0,
                "visit_gap": 3600.0,
                "can_see_sky": True,
            },
            {
                "obs_start": "2026-04-10T01:00:37.000",
                "band": "r",
                "physical_filter": "r_57",
                "overhead": 3600.0,
                "visit_gap": 7200.0,
                "can_see_sky": True,
            },
        )
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        # After sorting: r is first (no change), g is second (band change).
        assert result["sum_overhead_without_filter_change"] == pytest.approx(round(3600 / 3600, 2))
        assert result["sum_overhead_with_filter_change"] == pytest.approx(round(1800 / 3600, 2))

    def test_same_band_consecutive_goes_to_without_change_bucket(self):
        df = make_visits_df(
            {
                "obs_start": "2026-04-10T01:00:37.000",
                "band": "r",
                "physical_filter": "r_57",
                "overhead": 3600.0,
                "visit_gap": 7200.0,
            },
            {
                "obs_start": "2026-04-10T02:00:37.000",
                "band": "r",
                "physical_filter": "r_57",
                "overhead": 1800.0,
                "visit_gap": 3600.0,
            },
        )
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert result["sum_overhead_without_filter_change"] == pytest.approx(round((3600 + 1800) / 3600, 2))
        assert result["sum_overhead_with_filter_change"] == pytest.approx(0.0)

    def test_band_change_goes_to_with_change_bucket(self):
        df = make_visits_df(
            {
                "obs_start": "2026-04-10T01:00:37.000",
                "band": "r",
                "physical_filter": "r_57",
                "overhead": 3600.0,
                "visit_gap": 7200.0,
            },
            {
                "obs_start": "2026-04-10T02:00:37.000",
                "band": "g",
                "physical_filter": "g_6",
                "overhead": 1800.0,
                "visit_gap": 3600.0,
            },
        )
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert result["sum_overhead_without_filter_change"] == pytest.approx(round(3600 / 3600, 2))
        assert result["sum_overhead_with_filter_change"] == pytest.approx(round(1800 / 3600, 2))
        assert result["sum_visit_gap_without_filter_change"] == pytest.approx(round(7200 / 3600, 2))
        assert result["sum_visit_gap_with_filter_change"] == pytest.approx(round(3600 / 3600, 2))

    def test_physical_filter_change_same_band_stays_in_without_change_bucket(self):
        # band is the only discriminator -- physical_filter alone doesn't
        # trigger the with_change bucket.
        df = make_visits_df(
            {
                "obs_start": "2026-04-10T01:00:37.000",
                "band": "r",
                "physical_filter": "r_57",
                "overhead": 3600.0,
                "visit_gap": 7200.0,
            },
            {
                "obs_start": "2026-04-10T02:00:37.000",
                "band": "r",
                "physical_filter": "r_58",
                "overhead": 1800.0,
                "visit_gap": 3600.0,
            },
        )
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert result["sum_overhead_with_filter_change"] == pytest.approx(0.0)
        assert result["sum_overhead_without_filter_change"] == pytest.approx(round((3600 + 1800) / 3600, 2))

    def test_nan_overhead_treated_as_zero(self):
        df = make_visits_df({"overhead": None, "visit_gap": 3600.0})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert result["sum_overhead_without_filter_change"] == pytest.approx(0.0)
        assert result["sum_visit_gap_without_filter_change"] == pytest.approx(1.0)

    def test_unknown_dayobs_excluded(self):
        df = make_visits_df({"day_obs": 99990101, "can_see_sky": True})
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        assert all(v == 0.0 for v in result.values())

    def test_with_and_without_buckets_sum_to_total(self):
        df = make_visits_df(
            {
                "obs_start": "2026-04-10T01:00:37.000",
                "band": "r",
                "physical_filter": "r_57",
                "overhead": 3600.0,
                "visit_gap": 7200.0,
            },
            {
                "obs_start": "2026-04-10T02:00:37.000",
                "band": "g",
                "physical_filter": "g_6",
                "overhead": 1800.0,
                "visit_gap": 900.0,
            },
        )
        result = _sum_on_sky_within_twilight(df, ALMANAC_INFO)
        total_overhead = (
            result["sum_overhead_with_filter_change"] + result["sum_overhead_without_filter_change"]
        )
        total_gap = result["sum_visit_gap_with_filter_change"] + result["sum_visit_gap_without_filter_change"]
        assert total_overhead == pytest.approx(round((3600 + 1800) / 3600, 2))
        assert total_gap == pytest.approx(round((7200 + 900) / 3600, 2))


# ---------------------------------------------------------------------------
# get_time_accounting
# ---------------------------------------------------------------------------

MOCK_ALMANAC = ALMANAC_INFO

MOCK_VISITS_DF = make_visits_df(
    {
        "obs_start": OBS_START_IN_WINDOW_TAI,
        "band": "r",
        "physical_filter": "r_57",
        "overhead": 3600.0,
        "visit_gap": 7200.0,
        "can_see_sky": True,
    },
)


@pytest.fixture
def mock_time_accounting_deps():
    """Mock all external calls inside get_time_accounting."""

    def mock_augment_visits(exposures_df, instrument, skip_rs_columns=True):
        visits = exposures_df.copy()
        visits["slew_model"] = 5.0
        return visits

    def mock_add_model_slew_times(visits, efd, model_settle, dome_crawl=False):
        return visits.copy(), None

    with (
        patch(f"{MODULE}.get_clients", return_value={"efd": MagicMock()}),
        patch(f"{MODULE}.rn_aug.augment_visits", side_effect=mock_augment_visits),
        patch(f"{MODULE}.rn_sch.add_model_slew_times", side_effect=mock_add_model_slew_times),
        patch(f"{MODULE}.get_almanac", return_value=MOCK_ALMANAC),
    ):
        yield


class TestGetTimeAccounting:
    def test_empty_exposures_returns_zero_sums(self):
        result = get_time_accounting(20260409, 20260410, "lsstcam", [])
        assert set(result.keys()) == set(TWILIGHT_SUM_KEYS)
        assert all(v == 0.0 for v in result.values())

    def test_empty_exposures_does_not_call_external_services(self):
        with patch(f"{MODULE}.get_clients") as mock_clients:
            get_time_accounting(20260409, 20260410, "lsstcam", [])
            mock_clients.assert_not_called()

    def test_returns_all_four_keys(self, mock_time_accounting_deps):
        exposures = [make_visit()]
        result = get_time_accounting(20260409, 20260410, "lsstcam", exposures)
        assert set(result.keys()) == set(TWILIGHT_SUM_KEYS)

    def test_all_values_are_floats(self, mock_time_accounting_deps):
        exposures = [make_visit()]
        result = get_time_accounting(20260409, 20260410, "lsstcam", exposures)
        assert all(isinstance(v, float) for v in result.values())

    def test_non_on_sky_exposures_produce_zero_sums(self, mock_time_accounting_deps):
        exposures = [make_visit(can_see_sky=False)]
        result = get_time_accounting(20260409, 20260410, "lsstcam", exposures)
        assert all(v == 0.0 for v in result.values())

    def test_almanac_called_with_correct_dayobs_end(self, mock_time_accounting_deps):
        # almanac_dayobs_end should be dayObsEnd + 1 day.
        mock_visits = MOCK_VISITS_DF.copy()
        mock_visits["slew_model"] = 5.0

        with patch(f"{MODULE}.get_almanac", return_value=MOCK_ALMANAC) as mock_almanac:
            with patch(f"{MODULE}.rn_aug.augment_visits", return_value=mock_visits.copy()):
                with patch(
                    f"{MODULE}.rn_sch.add_model_slew_times",
                    return_value=(mock_visits.copy(), None),
                ):
                    get_time_accounting(20260409, 20260410, "lsstcam", [make_visit()])
        call_args = mock_almanac.call_args
        assert call_args[0][0] == 20260409  # dayObsStart unchanged
        assert call_args[0][1] == 20260411  # dayObsEnd + 1
