from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.services.rubin_nights_service import get_open_close_dome
from lsst.ts.logging_and_reporting.utils.dayobs import dayobs_at

MODULE = "lsst.ts.logging_and_reporting.services.rubin_nights_service"

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


def make_raw_dome_df(*sessions: dict) -> pd.DataFrame:
    """Build a raw per-session DataFrame."""
    return pd.DataFrame([make_session(**s) for s in sessions])


# ---------------------------------------------------------------------------
# dayobs_at
# ---------------------------------------------------------------------------


class TestDayobsAt:
    def test_after_noon_utc_returns_same_date(self):
        now = pd.Timestamp("2026-04-09 15:00:00", tz="UTC")
        assert dayobs_at(now) == 20260409

    def test_before_noon_utc_returns_previous_date(self):
        # 03:00 UTC April 10 is still dayobs 20260409
        now = pd.Timestamp("2026-04-10 03:00:00", tz="UTC")
        assert dayobs_at(now) == 20260409

    def test_exactly_at_noon_utc_returns_same_date(self):
        now = pd.Timestamp("2026-04-09 12:00:00", tz="UTC")
        assert dayobs_at(now) == 20260409

    def test_one_second_before_noon_returns_previous_date(self):
        now = pd.Timestamp("2026-04-09 11:59:59", tz="UTC")
        assert dayobs_at(now) == 20260408

    def test_month_boundary(self):
        now = pd.Timestamp("2026-05-01 03:00:00", tz="UTC")
        assert dayobs_at(now) == 20260430

    def test_year_boundary(self):
        now = pd.Timestamp("2027-01-01 03:00:00", tz="UTC")
        assert dayobs_at(now) == 20261231


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
