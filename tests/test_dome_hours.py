# tests/test_dome_closed_hours.py
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.web_app.services.rubin_nights_service import (
    _compute_closed_hours,
    _current_dayobs_utc,
    get_open_close_dome,
)

MODULE = "lsst.ts.logging_and_reporting.web_app.services.rubin_nights_service"

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

DAYOBS = 20260409
PREV_DAYOBS = 20260408
NEXT_DAYOBS = 20260410

SUNSET12 = pd.Timestamp("2026-04-09 23:00:00", tz="UTC")
SUNRISE12 = pd.Timestamp("2026-04-10 10:00:00", tz="UTC")
NIGHT_HOURS = (SUNRISE12 - SUNSET12).total_seconds() / 3600  # 11.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def make_session(**kwargs) -> dict:
    """A single dome open session row -- the unit
    get_dome_open_close returns.
    """
    defaults = {
        "day_obs": DAYOBS,
        "open_time": pd.Timestamp("2026-04-09 23:30:00"),
        "close_time": pd.Timestamp("2026-04-10 05:00:00"),
        "dome_hours": 5.5,
        "sunset12": pd.Timestamp("2026-04-09 23:00:00"),  # naive, UTC
        "sunrise12": pd.Timestamp("2026-04-10 10:00:00"),
        "night_hours": NIGHT_HOURS,
        "open_hours": 5.5,
    }
    return {**defaults, **kwargs}


def make_aggregated_row(**kwargs) -> pd.Series:
    """A per-night aggregated row -- what _compute_closed_hours operates on.
    Mirrors the output of groupby(...).agg(night_hours=max, open_hours=sum,
    sunset12=first, sunrise12=first).
    """
    defaults = {
        "night_hours": NIGHT_HOURS,
        "open_hours": 0.0,
        "sunset12": SUNSET12,
        "sunrise12": SUNRISE12,
        # day_obs is the groupby index, not a column, but we include it
        # in the Series so _compute_closed_hours can access it.
        "day_obs": DAYOBS,
    }
    return pd.Series({**defaults, **kwargs})


def make_raw_dome_df(*sessions: dict) -> pd.DataFrame:
    """Build a raw per-session DataFrame."""
    return pd.DataFrame([make_session(**s) for s in sessions])


# ---------------------------------------------------------------------------
# _current_dayobs_utc
# ---------------------------------------------------------------------------


class TestCurrentDayobsUtc:
    def test_after_noon_utc_returns_same_date(self):
        now = pd.Timestamp("2026-04-09 15:00:00", tz="UTC")
        assert _current_dayobs_utc(now) == 20260409

    def test_before_noon_utc_returns_previous_date(self):
        # 03:00 UTC April 10 is still dayobs 20260409
        now = pd.Timestamp("2026-04-10 03:00:00", tz="UTC")
        assert _current_dayobs_utc(now) == 20260409

    def test_exactly_at_noon_utc_returns_same_date(self):
        now = pd.Timestamp("2026-04-09 12:00:00", tz="UTC")
        assert _current_dayobs_utc(now) == 20260409

    def test_one_second_before_noon_returns_previous_date(self):
        now = pd.Timestamp("2026-04-09 11:59:59", tz="UTC")
        assert _current_dayobs_utc(now) == 20260408

    def test_month_boundary(self):
        now = pd.Timestamp("2026-05-01 03:00:00", tz="UTC")
        assert _current_dayobs_utc(now) == 20260430

    def test_year_boundary(self):
        now = pd.Timestamp("2027-01-01 03:00:00", tz="UTC")
        assert _current_dayobs_utc(now) == 20261231


# ---------------------------------------------------------------------------
# _compute_closed_hours (operates on aggregated per-night rows)
# ---------------------------------------------------------------------------


class TestComputeClosedHoursPastNight:
    def test_dome_did_not_open(self):
        row = make_aggregated_row(day_obs=PREV_DAYOBS, open_hours=0.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)

    def test_dome_open_for_full_night(self):
        row = make_aggregated_row(day_obs=PREV_DAYOBS, open_hours=11.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(0.0)

    def test_dome_open_for_partial_night(self):
        row = make_aggregated_row(day_obs=PREV_DAYOBS, open_hours=6.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(5.0)

    def test_multiple_sessions_aggregated_open_hours_sum(self):
        # Two sessions (3hrs + 2hrs = 5hrs) on a past night.
        # The groupby already summed them; _compute_closed_hours just
        # sees the total.
        row = make_aggregated_row(day_obs=PREV_DAYOBS, open_hours=5.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(6.0)

    def test_past_session_then_closed_then_second_session(self):
        # Dome opened 2hrs, closed 1hr, opened again 3hrs = 5hrs total open.
        row = make_aggregated_row(day_obs=PREV_DAYOBS, open_hours=5.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(6.0)

    def test_night_with_only_non_science_exposures(self):
        # Dome open/close is independent of exposure type.
        row = make_aggregated_row(day_obs=PREV_DAYOBS, open_hours=4.0, night_hours=11.0)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(7.0)


class TestComputeClosedHoursCurrentNight:
    def test_dome_not_yet_opened_uses_elapsed_since_sunset(self):
        # 2hrs into the night, dome still closed.
        row = make_aggregated_row(day_obs=DAYOBS, open_hours=0.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=2)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(2.0)

    def test_single_open_session_currently_active(self):
        # Dome has been open for 3hrs (rubin-nights tracks elapsed open
        # time for an active session). closed = night_hours - open_hours.
        row = make_aggregated_row(day_obs=DAYOBS, open_hours=3.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=5)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(NIGHT_HOURS - 3.0)

    def test_past_closed_session_then_current_open_session(self):
        # Session 1: dome open 1hr, closed 1hr. Session 2: currently open 3hrs.
        # Aggregated open_hours = 1 + 3 = 4.
        row = make_aggregated_row(day_obs=DAYOBS, open_hours=4.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=6)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(NIGHT_HOURS - 4.0)

    def test_multiple_past_open_sessions_no_current_open(self):
        # Two completed sessions (2hrs + 1.5hrs = 3.5hrs), dome now closed.
        # Because open_hours > 0, use night_hours - open_hours.
        row = make_aggregated_row(day_obs=DAYOBS, open_hours=3.5, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(hours=7)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(NIGHT_HOURS - 3.5)

    def test_just_after_evening_twilight_dome_closed(self):
        row = make_aggregated_row(day_obs=DAYOBS, open_hours=0.0, night_hours=NIGHT_HOURS)
        now_utc = SUNSET12 + timedelta(minutes=1)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(1 / 60)


class TestComputeClosedHoursEdgeCases:
    def test_future_night_not_yet_started(self):
        # now_utc is before sunset12 for a future dayobs -- not in progress,
        # falls back to night_hours - open_hours (both 0 for a future night).
        row = make_aggregated_row(
            day_obs=NEXT_DAYOBS,
            sunset12=pd.Timestamp("2026-04-10 23:00:00", tz="UTC"),
            sunrise12=pd.Timestamp("2026-04-11 10:00:00", tz="UTC"),
            open_hours=0.0,
            night_hours=11.0,
        )
        now_utc = SUNSET12 + timedelta(hours=1)  # still in previous night
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)

    def test_nat_sunset12_falls_back_to_past_night_branch(self):
        row = make_aggregated_row(
            day_obs=DAYOBS,
            sunset12=pd.NaT,
            open_hours=0.0,
            night_hours=11.0,
        )
        now_utc = SUNSET12 + timedelta(hours=3)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)

    def test_nat_sunrise12_falls_back_to_past_night_branch(self):
        row = make_aggregated_row(
            day_obs=DAYOBS,
            sunrise12=pd.NaT,
            open_hours=0.0,
            night_hours=11.0,
        )
        now_utc = SUNSET12 + timedelta(hours=3)
        assert _compute_closed_hours(row, DAYOBS, now_utc) == pytest.approx(11.0)


# ---------------------------------------------------------------------------
# get_open_close_dome (raw per-session return, no closed_hours)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_clients():
    with patch(f"{MODULE}.get_clients") as mock_get_clients:
        mock_get_clients.return_value = {"efd": MagicMock()}
        yield mock_get_clients


class TestGetOpenCloseDome:
    def test_single_session_night_returned(self, mock_clients):
        raw = make_raw_dome_df({"day_obs": DAYOBS, "open_hours": 5.5})
        with patch(f"{MODULE}.get_dome_open_close", return_value=raw):
            result = get_open_close_dome(DAYOBS, NEXT_DAYOBS, "lsstcam")
        assert len(result) == 1
        assert result.iloc[0]["day_obs"] == DAYOBS

    def test_multiple_sessions_same_night_both_rows_returned(self, mock_clients):
        # Two sessions on the same night -- both rows should be present
        # since closed_hours is not computed here; aggregation happens
        # in the endpoint groupby.
        raw = make_raw_dome_df(
            {
                "day_obs": DAYOBS,
                "open_hours": 3.0,
                "open_time": pd.Timestamp("2026-04-09 23:30:00"),
                "close_time": pd.Timestamp("2026-04-10 02:30:00"),
            },
            {
                "day_obs": DAYOBS,
                "open_hours": 2.0,
                "open_time": pd.Timestamp("2026-04-10 04:00:00"),
                "close_time": pd.Timestamp("2026-04-10 06:00:00"),
            },
        )
        with patch(f"{MODULE}.get_dome_open_close", return_value=raw):
            result = get_open_close_dome(DAYOBS, NEXT_DAYOBS, "lsstcam")
        assert len(result) == 2
        assert (result["day_obs"] == DAYOBS).all()

    def test_closed_night_no_sessions_returned(self, mock_clients):
        # get_dome_open_close now guarantees an entry even for closed nights.
        raw = make_raw_dome_df({"day_obs": DAYOBS, "open_hours": 0.0, "open_time": None, "close_time": None})
        with patch(f"{MODULE}.get_dome_open_close", return_value=raw):
            result = get_open_close_dome(DAYOBS, NEXT_DAYOBS, "lsstcam")
        assert len(result) == 1
        assert result.iloc[0]["open_hours"] == 0.0

    def test_closed_hours_not_in_raw_output(self, mock_clients):
        # closed_hours belongs on the aggregated per-night result,
        # not the per-session rows.
        raw = make_raw_dome_df({"day_obs": DAYOBS, "open_hours": 5.5})
        with patch(f"{MODULE}.get_dome_open_close", return_value=raw):
            result = get_open_close_dome(DAYOBS, NEXT_DAYOBS, "lsstcam")
        assert "closed_hours" not in result.columns

    def test_day_obs_as_index_normalised_to_column(self, mock_clients):
        raw = make_raw_dome_df({"day_obs": DAYOBS}).set_index("day_obs")
        with patch(f"{MODULE}.get_dome_open_close", return_value=raw):
            result = get_open_close_dome(DAYOBS, NEXT_DAYOBS, "lsstcam")
        assert "day_obs" in result.columns
        assert result.index.name != "day_obs"

    def test_multiple_nights_each_session_preserved(self, mock_clients):
        raw = make_raw_dome_df(
            {"day_obs": PREV_DAYOBS, "open_hours": 4.0},
            {"day_obs": DAYOBS, "open_hours": 3.0},
            {"day_obs": DAYOBS, "open_hours": 2.0},  # second session same night
        )
        with patch(f"{MODULE}.get_dome_open_close", return_value=raw):
            result = get_open_close_dome(PREV_DAYOBS, NEXT_DAYOBS, "lsstcam")
        assert len(result) == 3
        assert len(result[result["day_obs"] == DAYOBS]) == 2
        assert len(result[result["day_obs"] == PREV_DAYOBS]) == 1
