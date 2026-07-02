from datetime import UTC, datetime

import pytest

from lsst.ts.logging_and_reporting.web_app.services.almanac_service import (
    _compute_elapsed_twilight_hours,
)

EVENING_TWILIGHT = "2026-04-09T23:00:00+00:00"
MORNING_TWILIGHT = "2026-04-10T10:00:00+00:00"
NIGHT_HOURS = 11.0


class TestComputeElapsedTwilightHours:
    def test_future_night_returns_zero(self):
        now_utc = datetime(2026, 4, 9, 22, 0, tzinfo=UTC)
        assert _compute_elapsed_twilight_hours(
            NIGHT_HOURS,
            EVENING_TWILIGHT,
            MORNING_TWILIGHT,
            now_utc,
        ) == pytest.approx(0.0)

    def test_current_night_in_progress_returns_elapsed_hours(self):
        now_utc = datetime(2026, 4, 10, 2, 0, tzinfo=UTC)
        assert _compute_elapsed_twilight_hours(
            NIGHT_HOURS,
            EVENING_TWILIGHT,
            MORNING_TWILIGHT,
            now_utc,
        ) == pytest.approx(3.0)

    def test_completed_night_returns_full_night_hours(self):
        now_utc = datetime(2026, 4, 10, 12, 0, tzinfo=UTC)
        assert _compute_elapsed_twilight_hours(
            NIGHT_HOURS,
            EVENING_TWILIGHT,
            MORNING_TWILIGHT,
            now_utc,
        ) == pytest.approx(NIGHT_HOURS)

    def test_elapsed_hours_are_capped_at_night_hours(self):
        now_utc = datetime(2026, 4, 10, 2, 0, tzinfo=UTC)
        assert _compute_elapsed_twilight_hours(
            2.0,
            EVENING_TWILIGHT,
            MORNING_TWILIGHT,
            now_utc,
        ) == pytest.approx(2.0)
