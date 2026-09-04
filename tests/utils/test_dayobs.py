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

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    almanac_to_unix_ms,
    contiguous_runs,
    date_to_dayobs_int,
    dayobs_at,
    dayobs_int_to_date,
    dayobs_range,
    dayobs_to_unix_ms,
    get_utc_datetime_from_dayobs_str,
)

ONE_HOUR_UNIX_MS = 3_600_000


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


class TestDayobsDateConversion:
    def test_round_trip(self):
        assert date_to_dayobs_int(dayobs_int_to_date(20241230)) == 20241230

    def test_leap_day_is_valid(self):
        assert dayobs_int_to_date(20240229) == dt.date(2024, 2, 29)
        assert date_to_dayobs_int(dt.date(2024, 2, 29)) == 20240229

    def test_non_leap_year_feb_29_raises(self):
        with pytest.raises(ValueError):
            dayobs_int_to_date(20250229)

    def test_impossible_date_raises(self):
        with pytest.raises(ValueError):
            dayobs_int_to_date(20250230)  # Feb 30
        with pytest.raises(ValueError):
            dayobs_int_to_date(20251301)  # month 13


class TestDayobsRange:
    def test_within_month(self):
        assert dayobs_range(20250101, 20250103) == [20250101, 20250102, 20250103]

    def test_crosses_month_and_year(self):
        assert dayobs_range(20241230, 20250102) == [
            20241230,
            20241231,
            20250101,
            20250102,
        ]

    def test_single_day(self):
        assert dayobs_range(20250101, 20250101) == [20250101]


class TestContiguousRuns:
    def test_empty(self):
        assert contiguous_runs([]) == []

    def test_groups_and_sorts(self):
        assert contiguous_runs([20250104, 20250101, 20250103]) == [
            (20250101, 20250101),
            (20250103, 20250104),
        ]

    def test_calendar_aware(self):
        assert contiguous_runs([20250131, 20250201]) == [(20250131, 20250201)]

    def test_deduplicates(self):
        assert contiguous_runs([20250101, 20250101, 20250102]) == [(20250101, 20250102)]

    def test_single_day(self):
        assert contiguous_runs([20250101]) == [(20250101, 20250101)]

    def test_a_single_absent_day_splits_a_run(self):
        assert contiguous_runs([20250101, 20250103]) == [
            (20250101, 20250101),
            (20250103, 20250103),
        ]

    def test_calendar_aware_across_a_year(self):
        assert contiguous_runs([20241231, 20250101]) == [(20241231, 20250101)]


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


class TestGetUtcDatetimeFromDayobsStr:
    def test_hyphenated_date(self):
        actual = get_utc_datetime_from_dayobs_str("2024-10-14")
        assert actual == dt.datetime(2024, 10, 14, 12, 0, tzinfo=dt.timezone.utc)

    def test_compact_date(self):
        actual = get_utc_datetime_from_dayobs_str("20241014")
        assert actual == dt.datetime(2024, 10, 14, 12, 0, tzinfo=dt.timezone.utc)


class TestDayobsToUnixMs:
    def test_default_hour_is_noon_utc(self):
        assert dayobs_to_unix_ms(19700101) == 12 * ONE_HOUR_UNIX_MS

    def test_hour_is_configurable(self):
        assert dayobs_to_unix_ms(19700101, hour=0) == 0
        assert dayobs_to_unix_ms(19700101, hour=3) == 3 * ONE_HOUR_UNIX_MS

    def test_matches_datetime_timestamp(self):
        expected = int(dt.datetime(2025, 6, 15, 12, tzinfo=dt.timezone.utc).timestamp() * 1000)
        assert dayobs_to_unix_ms(20250615) == expected


class TestAlmanacToUnixMs:
    def test_converts_almanac_timestamp_string(self):
        assert almanac_to_unix_ms("1970-01-02 00:00:00") == 24 * ONE_HOUR_UNIX_MS

    def test_matches_datetime_timestamp(self):
        expected = int(dt.datetime(2025, 6, 15, 3, 30, 0, tzinfo=dt.timezone.utc).timestamp() * 1000)
        assert almanac_to_unix_ms("2025-06-15 03:30:00") == expected
