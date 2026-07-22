import datetime as dt

import pytest

from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    contiguous_runs,
    date_to_dayobs_int,
    dayobs_int_to_date,
    dayobs_range,
)


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
        assert dayobs_range(20241230, 20250102) == [20241230, 20241231, 20250101, 20250102]

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
