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

from datetime import timedelta
from unittest.mock import patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.services.exposures import (
    ExposuresService,
    _aggregate_dome_hours,
    _compute_closed_hours,
)

EXPOSURES = "lsst.ts.logging_and_reporting.services.exposures"


class StubConsdbAdapter:
    """Stands in for the ConsDB adapter; returns canned per-dayobs rows."""

    def __init__(self, per_day):
        self.per_day = per_day
        self.calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.calls.append((instrument, start_dayobs, end_dayobs))
        return self.per_day


class StubDomeAdapter:
    """Stands in for the dome adapter; returns canned per-dayobs sessions."""

    def __init__(self, per_day=None, error=None):
        self.per_day = per_day or {}
        self.error = error
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        if self.error is not None:
            raise self.error
        return self.per_day


class StubOverheadAdapter:
    """Stands in for the visit-overhead adapter; returns per-dayobs rows."""

    def __init__(self, per_day=None, error=None):
        self.per_day = per_day or {}
        self.error = error
        self.calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.calls.append((instrument, start_dayobs, end_dayobs))
        if self.error is not None:
            raise self.error
        return self.per_day


class StubAlmanacAdapter:
    """Stands in for the almanac adapter; returns per-dayobs records."""

    def __init__(self, records=None):
        self.records = records or {}
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        return self.records


def make_service(per_day, dome=None, overhead=None, almanac=None):
    return ExposuresService(
        consdb_adapter=StubConsdbAdapter(per_day),
        dome_adapter=dome or StubDomeAdapter(),
        overhead_adapter=overhead or StubOverheadAdapter(),
        almanac_adapter=almanac or StubAlmanacAdapter(),
    )


class TestCollation:
    def test_projects_curated_columns(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10, "extra_col": "drop me"}]}
        response = make_service(per_day).handle_request(20250101, 20250102, "LSSTCam")
        exposure = response["exposures"][0]
        assert "extra_col" not in exposure
        assert exposure["exposure_id"] == 1
        assert exposure["band"] is None  # curated column absent from the record

    def test_counts_and_durations(self):
        per_day = {
            20250101: [
                {"exposure_id": 1, "seq_num": 1, "can_see_sky": True, "exp_time": 10},
                {"exposure_id": 2, "seq_num": 2, "can_see_sky": False, "exp_time": 20},
                {"exposure_id": 3, "seq_num": 3, "can_see_sky": True, "exp_time": 5},
            ]
        }
        response = make_service(per_day).handle_request(20250101, 20250102, "LSSTCam")
        assert response["exposures_count"] == 3
        assert response["sum_exposure_time"] == 35
        assert response["on_sky_exposures_count"] == 2
        assert response["total_on_sky_exposure_time"] == 15

    def test_orders_by_dayobs_then_seq_num(self):
        per_day = {
            20250102: [{"exposure_id": 4, "seq_num": 1}],
            20250101: [{"exposure_id": 2, "seq_num": 2}, {"exposure_id": 1, "seq_num": 1}],
        }
        response = make_service(per_day).handle_request(20250101, 20250103, "LSSTCam")
        assert [exposure["exposure_id"] for exposure in response["exposures"]] == [1, 2, 4]


class TestExclusiveEnd:
    def test_converts_exclusive_end_to_inclusive_before_fetch(self):
        consdb = StubConsdbAdapter({})
        dome = StubDomeAdapter()
        overhead = StubOverheadAdapter()
        service = ExposuresService(
            consdb_adapter=consdb,
            dome_adapter=dome,
            overhead_adapter=overhead,
            almanac_adapter=StubAlmanacAdapter(),
        )
        service.handle_request(20250101, 20250103, "LSSTCam")
        # Dayobs adapters get the inclusive end (exclusive end - 1 day).
        assert consdb.calls == [("LSSTCam", 20250101, 20250102)]
        assert dome.calls == [(20250101, 20250102)]
        assert overhead.calls == [("LSSTCam", 20250101, 20250102)]


class TestGracefulDegradation:
    def test_dome_failure_reports_error_and_keeps_exposures(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        dome = StubDomeAdapter(error=RuntimeError("dome down"))
        response = make_service(per_day, dome).handle_request(20250101, 20250102, "LSSTCam")
        assert response["open_dome_error"] == "Failed to retrieve dome open/close times"
        assert response["exposures_count"] == 1

    def test_dome_aggregation_failure_reports_error_but_keeps_times(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        dome = StubDomeAdapter({20250730: [{"day_obs": 20250730, "night_hours": 12.0, "open_hours": 9.0}]})
        with patch(f"{EXPOSURES}._aggregate_dome_hours", side_effect=RuntimeError("aggregation failed")):
            response = make_service(per_day, dome).handle_request(20250101, 20250102, "LSSTCam")
        assert response["open_dome_error"] == "Failed to aggregate dome open hours"
        assert response["day_obs_open_dome_hours"] is None
        assert len(response["open_dome_times"]) == 1  # retrieval succeeded

    def test_time_accounting_failure_reports_error(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        overhead = StubOverheadAdapter(error=RuntimeError("overhead down"))
        response = make_service(per_day, overhead=overhead).handle_request(20250101, 20250102, "LSSTCam")
        assert response["time_accounting_error"] == "Failed to compute night time accounting"
        assert response["night_on_sky_time_accounting"] is None

    def test_time_accounting_empty_when_no_overhead_rows(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        almanac = StubAlmanacAdapter()
        response = make_service(per_day, almanac=almanac).handle_request(20250101, 20250102, "LSSTCam")
        assert response["night_on_sky_time_accounting"] == {
            "sum_overhead_with_filter_change": 0.0,
            "sum_overhead_without_filter_change": 0.0,
            "sum_visit_gap_with_filter_change": 0.0,
            "sum_visit_gap_without_filter_change": 0.0,
        }
        assert response["time_accounting_error"] is None
        # The almanac is not consulted when there are no overhead rows.
        assert almanac.calls == []

    def test_time_accounting_computed_from_overhead_and_almanac(self):
        per_day = {20260409: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        overhead = StubOverheadAdapter(
            {
                20260409: [
                    {
                        "day_obs": 20260409,
                        "obs_start": "2026-04-10T01:00:37.000000",  # UTC 01:00, inside twilight
                        "can_see_sky": True,
                        "band": "r",
                        "overhead": 3600.0,  # 1 hour
                        "visit_gap": 7200.0,  # 2 hours
                    }
                ]
            }
        )
        almanac = StubAlmanacAdapter(
            {
                20260410: {  # labeled by morning twilight -> maps to visit dayobs 20260409
                    "dayobs": 20260410,
                    "twilight_evening_12deg": "2026-04-09 23:00:00",
                    "twilight_morning_12deg": "2026-04-10 10:00:00",
                }
            }
        )
        response = make_service(per_day, overhead=overhead, almanac=almanac).handle_request(
            20260409, 20260410, "LSSTCam"
        )
        accounting = response["night_on_sky_time_accounting"]
        assert accounting["sum_overhead_without_filter_change"] == pytest.approx(1.0)
        assert accounting["sum_visit_gap_without_filter_change"] == pytest.approx(2.0)
        assert response["time_accounting_error"] is None
        assert overhead.calls == [("LSSTCam", 20260409, 20260409)]
        assert almanac.calls == [(20260409, 20260410)]


class TestDomeAggregation:
    def test_open_dome_times_and_per_night_hours(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        session = {
            "day_obs": 20250730,
            "open_time": "2025-07-30T23:11:57+00:00",
            "close_time": "2025-07-31T08:15:00+00:00",
            "dome_hours": 9.06,
            "night_hours": 12.0,
            "open_hours": 9.06,
            "sunset12": "2025-07-30T23:00:00+00:00",
            "sunrise12": "2025-07-31T11:00:00+00:00",
        }
        dome = StubDomeAdapter({20250730: [session]})
        response = make_service(per_day, dome).handle_request(20250101, 20250102, "LSSTCam")

        assert response["open_dome_times"] == [session]
        night = response["day_obs_open_dome_hours"][20250730]
        assert night["night_hours"] == 12.0
        assert night["open_hours"] == 9.06
        # A past night, so closed = night_hours - open_hours.
        assert night["closed_hours"] == pytest.approx(12.0 - 9.06)
        assert response["open_dome_error"] is None


# ---------------------------------------------------------------------------
# _compute_closed_hours (operates on one aggregated per-night row)
# ---------------------------------------------------------------------------

DAYOBS = 20260409
PREV_DAYOBS = 20260408
NEXT_DAYOBS = 20260410

SUNSET12 = pd.Timestamp("2026-04-09 23:00:00", tz="UTC")
SUNRISE12 = pd.Timestamp("2026-04-10 10:00:00", tz="UTC")
NIGHT_HOURS = (SUNRISE12 - SUNSET12).total_seconds() / 3600  # 11.0


def totals(**kwargs) -> dict:
    """One aggregated per-night row, as `_aggregate_dome_hours` builds."""
    defaults = {
        "day_obs": DAYOBS,
        "night_hours": NIGHT_HOURS,
        "open_hours": 0.0,
        "sunset12": SUNSET12,
        "sunrise12": SUNRISE12,
    }
    return {**defaults, **kwargs}


class TestComputeClosedHoursPastNight:
    def test_dome_did_not_open(self):
        row = totals(day_obs=PREV_DAYOBS, open_hours=0.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)

    def test_dome_open_for_full_night(self):
        row = totals(day_obs=PREV_DAYOBS, open_hours=11.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(0.0)

    def test_dome_open_for_partial_night(self):
        row = totals(day_obs=PREV_DAYOBS, open_hours=6.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(5.0)

    def test_multiple_sessions_aggregated_open_hours_sum(self):
        row = totals(day_obs=PREV_DAYOBS, open_hours=5.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(6.0)

    def test_past_session_then_closed_then_second_session(self):
        # Dome opened 2hrs, closed 1hr, opened again 3hrs = 5hrs total open.
        row = totals(day_obs=PREV_DAYOBS, open_hours=5.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(6.0)

    def test_night_with_only_non_science_exposures(self):
        # Dome open/close is independent of exposure type.
        row = totals(day_obs=PREV_DAYOBS, open_hours=4.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(7.0)


class TestComputeClosedHoursCurrentNight:
    def test_dome_not_yet_opened_uses_elapsed_since_sunset(self):
        row = totals(day_obs=DAYOBS, open_hours=0.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(2.0)

    def test_single_open_session_currently_active(self):
        row = totals(day_obs=DAYOBS, open_hours=3.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=5)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(2.0)

    def test_past_closed_session_then_current_open_session(self):
        # Session 1: dome open 1hr, closed 1hr. Session 2: currently open 3hrs.
        # Aggregated open_hours = 1 + 3 = 4, elapsed twilight = 6hrs,
        # so closed so far is 2hrs.
        row = totals(day_obs=DAYOBS, open_hours=4.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=6)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(2.0)

    def test_multiple_past_open_sessions_no_current_open(self):
        # Two completed sessions (2hrs + 1.5hrs = 3.5hrs), dome now closed,
        # 7hrs into the night, so closed so far is 3.5hrs.
        row = totals(day_obs=DAYOBS, open_hours=3.5, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=7)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(3.5)

    def test_just_after_evening_twilight_dome_closed(self):
        row = totals(day_obs=DAYOBS, open_hours=0.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(minutes=1)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(1 / 60)

    def test_open_hours_greater_than_elapsed_clamps_to_zero(self):
        row = totals(day_obs=DAYOBS, open_hours=3.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(0.0)


class TestComputeClosedHoursEdgeCases:
    def test_future_night_not_yet_started(self):
        row = totals(
            day_obs=NEXT_DAYOBS,
            sunset12=pd.Timestamp("2026-04-10 23:00:00", tz="UTC"),
            sunrise12=pd.Timestamp("2026-04-11 10:00:00", tz="UTC"),
            open_hours=0.0,
            night_hours=11.0,
        )
        now_utc = SUNSET12 + timedelta(hours=1)  # still in the previous night
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)

    def test_nat_sunset12_falls_back_to_past_night_branch(self):
        row = totals(day_obs=DAYOBS, sunset12=pd.NaT, open_hours=0.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=3)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)

    def test_nat_sunrise12_falls_back_to_past_night_branch(self):
        row = totals(day_obs=DAYOBS, sunrise12=pd.NaT, open_hours=0.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=3)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)


# ---------------------------------------------------------------------------
# _aggregate_dome_hours (per-dayobs sessions -> per-night hours)
# ---------------------------------------------------------------------------

# A safely-past night, so "night in progress" is never true regardless of
# when the suite runs and closed_hours == night_hours - open_hours.
PAST_DAYOBS = 20250115


def dome_record(**kwargs) -> dict:
    defaults = {
        "day_obs": PAST_DAYOBS,
        "open_time": "2025-01-15T23:30:00+00:00",
        "close_time": "2025-01-16T05:00:00+00:00",
        "dome_hours": 5.5,
        "night_hours": 11.0,
        "open_hours": 5.5,
        "sunset12": "2025-01-15T23:00:00+00:00",
        "sunrise12": "2025-01-16T10:00:00+00:00",
    }
    return {**defaults, **kwargs}


class TestAggregateDomeHours:
    def test_single_session(self):
        hours = _aggregate_dome_hours({PAST_DAYOBS: [dome_record(open_hours=5.5, night_hours=11.0)]})
        assert hours[PAST_DAYOBS]["night_hours"] == 11.0
        assert hours[PAST_DAYOBS]["open_hours"] == 5.5
        assert hours[PAST_DAYOBS]["closed_hours"] == pytest.approx(5.5)

    def test_multiple_sessions_sum_open_and_take_first_twilight(self):
        hours = _aggregate_dome_hours(
            {
                PAST_DAYOBS: [
                    dome_record(open_hours=3.0, sunset12="2025-01-15T23:00:00+00:00"),
                    dome_record(open_hours=2.0, sunset12="2025-01-15T23:30:00+00:00"),
                ]
            }
        )
        assert hours[PAST_DAYOBS]["open_hours"] == pytest.approx(5.0)
        assert hours[PAST_DAYOBS]["night_hours"] == 11.0
        assert hours[PAST_DAYOBS]["sunset12"] == "2025-01-15T23:00:00+00:00"  # from the first session
        assert hours[PAST_DAYOBS]["closed_hours"] == pytest.approx(6.0)

    def test_night_with_no_sessions_is_omitted(self):
        assert _aggregate_dome_hours({PAST_DAYOBS: []}) == {}
