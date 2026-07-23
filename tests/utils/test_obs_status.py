from datetime import datetime, timezone

from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days
from lsst.ts.logging_and_reporting.utils.obs_status import (
    MILLISECONDS_IN_AN_HOUR,
    OBS_STATUS_AVAILABLE_DAYOBS,
    OBSERVATORY_STATES,
    almanac_to_unix_ms,
    build_ms_dayobs_intervals,
    build_ms_night_intervals,
    contains_downtime,
    contains_fault,
    contains_idle,
    counts_as_fault_loss,
    dayobs_to_unix_ms,
    decode_states,
    get_availability,
    get_obs_status_intervals,
    is_unknown,
    sum_interval_overlap,
)

# To simplify interval computation, default intervals start at
# 0 unix ms, which represents 1970-01-01 00:00:00.
AVAILABLE_DAYOBS = OBS_STATUS_AVAILABLE_DAYOBS
ONE_HOUR_UNIX_MS = MILLISECONDS_IN_AN_HOUR

DEFAULT_EVENT_INTERVAL_HR = 1
DEFAULT_EVENT_INTERVAL_MS = ONE_HOUR_UNIX_MS * DEFAULT_EVENT_INTERVAL_HR

DEFAULT_EVENT_INTERVAL_START_MS = 0
DEFAULT_EVENT_INTERVAL_END_MS = DEFAULT_EVENT_INTERVAL_START_MS + DEFAULT_EVENT_INTERVAL_MS

DEFAULT_NIGHT_INTERVAL_HR = 10
DEFAULT_NIGHT_INTERVAL_MS = ONE_HOUR_UNIX_MS * DEFAULT_NIGHT_INTERVAL_HR

DEFAULT_NIGHT_START_MS = 0
DEFAULT_NIGHT_END_MS = DEFAULT_NIGHT_START_MS + DEFAULT_NIGHT_INTERVAL_MS

# For multi-night tests, we use a second night interval.
SECOND_NIGHT_START_MS = DEFAULT_NIGHT_END_MS + DEFAULT_NIGHT_INTERVAL_MS
SECOND_NIGHT_END_MS = SECOND_NIGHT_START_MS + DEFAULT_NIGHT_INTERVAL_MS


def unix_ms_to_dt(unix_ms: int) -> str:
    """Convert a unix ms timestamp to a UTC timestamp string
    (YYYY-MM-DD HH:MM:SS).
    """
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def unix_ms_to_dayobs_int(unix_ms: int) -> int:
    """Convert a unix ms timestamp to a dayobs integer (YYYYMMDD)."""
    return int(datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc).strftime("%Y%m%d"))


DEFAULT_NIGHT_START_DT = unix_ms_to_dt(DEFAULT_NIGHT_START_MS)
DEFAULT_NIGHT_END_DT = unix_ms_to_dt(DEFAULT_NIGHT_END_MS)

SECOND_NIGHT_START_DT = unix_ms_to_dt(SECOND_NIGHT_START_MS)
SECOND_NIGHT_END_DT = unix_ms_to_dt(DEFAULT_NIGHT_END_MS)

DEFAULT_START_DAYOBS_INT = unix_ms_to_dayobs_int(DEFAULT_NIGHT_START_MS)
DEFAULT_END_DAYOBS_INT = unix_ms_to_dayobs_int(DEFAULT_NIGHT_END_MS)


# Helpers for creating inputs to pass in to various tested functions.
def make_event(status=1, time_ms=0, time="foo", note="foo", labels=None):
    return {
        "status": status,
        "time_ms": time_ms,
        "time": time,
        "note": note,
        "statusLabels": labels or [],
    }


def make_event_interval(
    start_state=OBSERVATORY_STATES["OPERATIONAL"],
    start_time_ms=DEFAULT_EVENT_INTERVAL_START_MS,
    end_time_ms=DEFAULT_EVENT_INTERVAL_END_MS,
):
    return {
        "start_state": start_state,
        "start_time_ms": start_time_ms,
        "end_time_ms": end_time_ms,
    }


def make_night_interval(
    start_time_ms=DEFAULT_NIGHT_START_MS,
    end_time_ms=DEFAULT_NIGHT_END_MS,
):
    return {"start_ms": start_time_ms, "end_ms": end_time_ms}


def make_almanac():
    return [
        {
            "twilight_evening_12deg": DEFAULT_NIGHT_START_DT,
            "twilight_morning_12deg": DEFAULT_NIGHT_END_DT,
        }
    ]


class TestIntervals:
    """Tests for interval construction and overlap logic."""

    def test_get_obs_status_intervals_single(self):
        events = [
            make_event(status=OBSERVATORY_STATES["DAYTIME"]),
            make_event(status=OBSERVATORY_STATES["OPERATIONAL"], time_ms=ONE_HOUR_UNIX_MS),
        ]

        result = get_obs_status_intervals(events)

        assert len(result) == 1
        assert result[0]["interval_length_ms"] == ONE_HOUR_UNIX_MS
        assert result[0]["start_state"] == OBSERVATORY_STATES["DAYTIME"]
        assert result[0]["end_state"] == OBSERVATORY_STATES["OPERATIONAL"]
        assert result[0]["changed_mask"] == OBSERVATORY_STATES["DAYTIME"] | OBSERVATORY_STATES["OPERATIONAL"]

    def test_get_obs_status_intervals_no_events(self):
        assert get_obs_status_intervals([]) == []

    def test_get_obs_status_intervals_single_event(self):
        # A lone event yields no interval (intervals pair consecutive events).
        assert get_obs_status_intervals([make_event()]) == []

    def test_get_obs_status_intervals_multiple(self):
        events = [
            make_event(status=OBSERVATORY_STATES["DAYTIME"], time_ms=0),
            make_event(status=OBSERVATORY_STATES["OPERATIONAL"], time_ms=ONE_HOUR_UNIX_MS),
            make_event(status=OBSERVATORY_STATES["FAULT"], time_ms=2 * ONE_HOUR_UNIX_MS),
        ]

        result = get_obs_status_intervals(events)

        assert len(result) == 2
        assert result[1]["start_state"] == OBSERVATORY_STATES["OPERATIONAL"]
        assert result[1]["end_state"] == OBSERVATORY_STATES["FAULT"]

    def test_get_obs_status_intervals_activate_unknown(self):
        """UNKNOWN should appear as activated when transitioning
        from a known state into UNKNOWN.
        """
        events = [
            make_event(status=OBSERVATORY_STATES["OPERATIONAL"]),
            make_event(status=OBSERVATORY_STATES["UNKNOWN"], time_ms=ONE_HOUR_UNIX_MS),
        ]

        interval = get_obs_status_intervals(events)[0]

        assert interval["activated"] == OBSERVATORY_STATES["UNKNOWN"]
        assert interval["deactivated"] == OBSERVATORY_STATES["OPERATIONAL"]
        assert interval["activated_labels"] == ["UNKNOWN"]
        assert interval["deactivated_labels"] == ["OPERATIONAL"]

    def test_get_obs_status_intervals_deactivate_unknown(self):
        """UNKNOWN should appear as deactivated when transitioning
        from UNKNOWN into a known state.
        """
        events = [
            make_event(status=OBSERVATORY_STATES["UNKNOWN"]),
            make_event(status=OBSERVATORY_STATES["OPERATIONAL"], time_ms=ONE_HOUR_UNIX_MS),
        ]

        interval = get_obs_status_intervals(events)[0]

        assert interval["activated"] == OBSERVATORY_STATES["OPERATIONAL"]
        assert interval["deactivated"] == OBSERVATORY_STATES["UNKNOWN"]
        assert interval["activated_labels"] == ["OPERATIONAL"]
        assert interval["deactivated_labels"] == ["UNKNOWN"]

    def test_get_obs_status_intervals_change_states_no_unknown(self):
        """Adding/removing a bit to/from an existing known status
        should not incorrectly activate/deactivate UNKNOWN.
        """
        events = [
            make_event(status=(OBSERVATORY_STATES["OPERATIONAL"] | OBSERVATORY_STATES["WEATHER"])),
            make_event(
                status=(OBSERVATORY_STATES["FAULT"] | OBSERVATORY_STATES["WEATHER"]),
                time_ms=ONE_HOUR_UNIX_MS,
            ),
        ]

        interval = get_obs_status_intervals(events)[0]

        assert interval["activated"] == OBSERVATORY_STATES["FAULT"]
        assert interval["deactivated"] == OBSERVATORY_STATES["OPERATIONAL"]
        assert interval["activated_labels"] == ["FAULT"]
        assert interval["deactivated_labels"] == ["OPERATIONAL"]

    def test_get_obs_status_unknown_to_unknown(self):
        """No-change UNKNOWN intervals should not report any
        activated or deactivated labels.
        """
        events = [
            make_event(status=OBSERVATORY_STATES["UNKNOWN"]),
            make_event(status=OBSERVATORY_STATES["UNKNOWN"], time_ms=ONE_HOUR_UNIX_MS),
        ]

        interval = get_obs_status_intervals(events)[0]

        assert interval["has_changed"] is False
        assert interval["changed_mask"] == 0
        assert interval["activated"] is None
        assert interval["deactivated"] is None
        assert interval["activated_labels"] == []
        assert interval["deactivated_labels"] == []

    def test_get_obs_status_intervals_no_change(self):
        """No-change intervals should not report any activated
        or deactivated labels.
        """
        events = [
            make_event(status=OBSERVATORY_STATES["FAULT"]),
            make_event(status=OBSERVATORY_STATES["FAULT"], time_ms=ONE_HOUR_UNIX_MS),
        ]

        interval = get_obs_status_intervals(events)[0]

        assert interval["has_changed"] is False
        assert interval["changed_mask"] == 0
        assert interval["activated"] is None
        assert interval["deactivated"] is None
        assert interval["activated_labels"] == []
        assert interval["deactivated_labels"] == []

    def test_decode_states_single_bitmask(self):
        decoded = decode_states(OBSERVATORY_STATES["FAULT"])
        assert "FAULT" in decoded
        assert isinstance(decoded, list)

    def test_decode_states_combined_bitmask(self):
        decoded = decode_states(OBSERVATORY_STATES["FAULT"] | OBSERVATORY_STATES["WEATHER"])
        assert "FAULT" in decoded
        assert "WEATHER" in decoded

    def test_decode_states_unknown(self):
        decoded = decode_states(OBSERVATORY_STATES["UNKNOWN"])
        assert "UNKNOWN" in decoded
        assert isinstance(decoded, list)


class TestAlmanacAndDayobsHandling:
    """Tests for almanac and dayobs conversion to ms intervals."""

    def test_build_ms_night_intervals(self):
        assert build_ms_night_intervals(make_almanac()) == [make_night_interval()]

    def test_build_ms_night_intervals_mulitple_nights(self):
        nights_dt = make_almanac()
        nights_dt.append(
            {
                "twilight_evening_12deg": SECOND_NIGHT_START_DT,
                "twilight_morning_12deg": SECOND_NIGHT_END_DT,
            }
        )

        result = build_ms_night_intervals(nights_dt)

        assert result == [
            make_night_interval(),
            make_night_interval(SECOND_NIGHT_START_MS, DEFAULT_NIGHT_END_MS),
        ]

    def test_build_ms_dayobs_intervals(self):
        result = build_ms_dayobs_intervals(DEFAULT_START_DAYOBS_INT, DEFAULT_END_DAYOBS_INT)

        assert result[0]["start_ms"] == DEFAULT_NIGHT_START_MS + (12 * ONE_HOUR_UNIX_MS)
        assert result[0]["end_ms"] == DEFAULT_NIGHT_START_MS + (36 * ONE_HOUR_UNIX_MS) - 1000

    def test_almanac_to_unix_ms(self):
        assert almanac_to_unix_ms(DEFAULT_NIGHT_END_DT) == DEFAULT_NIGHT_END_MS

    def test_dayobs_to_unix_ms_default(self):
        result = dayobs_to_unix_ms(DEFAULT_START_DAYOBS_INT)
        assert result == DEFAULT_NIGHT_START_MS + (12 * ONE_HOUR_UNIX_MS)

    def test_dayobs_to_unix_ms_various_cases(self):
        for hour in (0, 3, 12):
            result = dayobs_to_unix_ms(DEFAULT_START_DAYOBS_INT, hour)
            assert result == DEFAULT_NIGHT_START_MS + (hour * ONE_HOUR_UNIX_MS)


class TestPredicates:
    """Tests for status predicate functions."""

    def test_is_unknown(self):
        assert is_unknown(OBSERVATORY_STATES["UNKNOWN"])

    def test_contains_fault_true(self):
        assert contains_fault(OBSERVATORY_STATES["FAULT"])

    def test_contains_fault_false(self):
        assert not contains_fault(OBSERVATORY_STATES["DAYTIME"])

    def test_contains_idle_true(self):
        assert contains_idle(OBSERVATORY_STATES["IDLE"])

    def test_contains_idle_is_not_downtime(self):
        # IDLE and DOWNTIME are distinct states; IDLE must not match DOWNTIME.
        assert not contains_idle(OBSERVATORY_STATES["DOWNTIME"])

    def test_contains_downtime(self):
        assert contains_downtime(OBSERVATORY_STATES["DOWNTIME"])
        assert not contains_downtime(OBSERVATORY_STATES["IDLE"])

    def test_counts_as_fault_loss_true(self):
        status = OBSERVATORY_STATES["FAULT"] | OBSERVATORY_STATES["WEATHER"]
        assert counts_as_fault_loss(status)

    def test_counts_as_fault_loss_false(self):
        status = OBSERVATORY_STATES["FAULT"] | OBSERVATORY_STATES["DOWNTIME"]
        assert not counts_as_fault_loss(status)


class TestSumIntervalOverlap:
    """Tests for interval overlap computation."""

    def test_sum_interval_overlap_full(self):
        result = sum_interval_overlap([make_event_interval()], [make_night_interval()], lambda _: True)
        assert result == DEFAULT_EVENT_INTERVAL_HR

    def test_sum_interval_overlap_partial_start(self):
        # The event interval symmetrically straddles the start of the night.
        foo = DEFAULT_EVENT_INTERVAL_MS / 2
        night_intervals = [
            make_night_interval(
                start_time_ms=DEFAULT_NIGHT_START_MS + foo,
                end_time_ms=DEFAULT_NIGHT_END_MS + foo,
            )
        ]

        result = sum_interval_overlap([make_event_interval()], night_intervals, lambda _: True)

        assert result == (DEFAULT_EVENT_INTERVAL_HR / 2)

    def test_sum_interval_overlap_partial_end(self):
        # The event interval symmetrically straddles the end of the night.
        foo = DEFAULT_EVENT_INTERVAL_MS / 2
        event_intervals = [
            make_event_interval(
                start_time_ms=DEFAULT_NIGHT_END_MS - foo,
                end_time_ms=DEFAULT_NIGHT_END_MS + foo,
            )
        ]

        result = sum_interval_overlap(event_intervals, [make_night_interval()], lambda _: True)

        assert result == 0.5

    def test_sum_interval_no_overlap(self):
        event_intervals = [
            make_event_interval(
                start_time_ms=DEFAULT_NIGHT_END_MS,
                end_time_ms=DEFAULT_NIGHT_END_MS + DEFAULT_EVENT_INTERVAL_MS,
            )
        ]

        result = sum_interval_overlap(event_intervals, [make_night_interval()], lambda _: True)

        assert result == 0

    def test_sum_interval_overlap_filtered_out(self):
        # There is overlap, but the predicate returns false.
        result = sum_interval_overlap([make_event_interval()], [make_night_interval()], lambda _: False)
        assert result == 0

    def test_sum_interval_overlap_empty(self):
        assert sum_interval_overlap([], [make_night_interval()], lambda _: True) == 0

    def test_sum_interval_overlap_multiple_nights(self):
        # Multiple nights overlap with events; tests the loop over nights.
        event_intervals = [
            make_event_interval(),
            make_event_interval(
                start_time_ms=DEFAULT_NIGHT_END_MS,
                end_time_ms=DEFAULT_NIGHT_END_MS + DEFAULT_EVENT_INTERVAL_MS,
            ),
            make_event_interval(
                start_time_ms=SECOND_NIGHT_START_MS,
                end_time_ms=SECOND_NIGHT_START_MS + DEFAULT_EVENT_INTERVAL_MS,
            ),
        ]

        night_intervals = [
            make_night_interval(),
            make_night_interval(start_time_ms=SECOND_NIGHT_START_MS, end_time_ms=SECOND_NIGHT_END_MS),
        ]

        result = sum_interval_overlap(event_intervals, night_intervals, lambda _: True)

        assert result == 2

    def test_sum_interval_overlap_multinight_interval(self):
        # Edge case: one event interval overlaps multiple nights.
        event_intervals = [
            make_event_interval(end_time_ms=SECOND_NIGHT_START_MS + DEFAULT_EVENT_INTERVAL_MS),
        ]

        night_intervals = [
            make_night_interval(),
            make_night_interval(start_time_ms=SECOND_NIGHT_START_MS, end_time_ms=SECOND_NIGHT_END_MS),
        ]

        result = sum_interval_overlap(event_intervals, night_intervals, lambda _: True)

        assert result == DEFAULT_NIGHT_INTERVAL_HR + DEFAULT_EVENT_INTERVAL_HR


class TestGetAvailability:
    """Tests for determining data availability.

    Observatory Status data became available on 2026-02-25.
    """

    def test_get_availability_full(self):
        dayobs_start = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, 7)
        dayobs_end = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, 14)

        availability = get_availability(dayobs_start, dayobs_end)

        assert availability["status"] == "full"
        assert availability["available_from"] == AVAILABLE_DAYOBS

    def test_get_availability_none(self):
        dayobs_start = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, -14)
        dayobs_end = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, -7)

        assert get_availability(dayobs_start, dayobs_end)["status"] == "none"

    def test_get_availability_partial(self):
        dayobs_start = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, -7)
        dayobs_end = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, 7)

        assert get_availability(dayobs_start, dayobs_end)["status"] == "partial"

    def test_get_availability_starts_on_boundary(self):
        dayobs_end = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, 7)
        assert get_availability(AVAILABLE_DAYOBS, dayobs_end)["status"] == "full"

    def test_get_availability_ends_before_boundary(self):
        dayobs_start = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, -7)
        dayobs_end = add_or_subtract_dayobs_days(AVAILABLE_DAYOBS, -1)

        assert get_availability(dayobs_start, dayobs_end)["status"] == "none"
