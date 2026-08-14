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

from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days


class TestAddOrSubtractDayobsDays:
    """Tests for add_or_subtract_dayobs_days."""

    def test_add_day(self):
        assert add_or_subtract_dayobs_days(20260519, 1) == 20260520

    def test_subtract_day(self):
        assert add_or_subtract_dayobs_days(20260520, -1) == 20260519

    def test_month_rollover_forward(self):
        assert add_or_subtract_dayobs_days(20260531, 1) == 20260601

    def test_month_rollover_backward(self):
        assert add_or_subtract_dayobs_days(20260601, -1) == 20260531

    def test_year_rollover_forward(self):
        assert add_or_subtract_dayobs_days(20251231, 1) == 20260101

    def test_year_rollover_backward(self):
        assert add_or_subtract_dayobs_days(20260101, -1) == 20251231

    def test_leap_year_forward(self):
        assert add_or_subtract_dayobs_days(20240228, 1) == 20240229

    def test_leap_year_backward(self):
        assert add_or_subtract_dayobs_days(20240301, -1) == 20240229

    def test_non_leap_year_forward(self):
        assert add_or_subtract_dayobs_days(20230228, 1) == 20230301

    def test_non_leap_year_backward(self):
        assert add_or_subtract_dayobs_days(20230301, -1) == 20230228

    def test_zero_days(self):
        assert add_or_subtract_dayobs_days(20260519, 0) == 20260519

    def test_multiple_days_forward(self):
        assert add_or_subtract_dayobs_days(20260519, 10) == 20260529

    def test_multiple_days_backward(self):
        assert add_or_subtract_dayobs_days(20260519, -10) == 20260509
