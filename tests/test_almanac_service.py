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
