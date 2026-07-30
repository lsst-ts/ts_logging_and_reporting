from datetime import UTC, datetime

import pytest

from lsst.ts.logging_and_reporting.services.almanac import (
    AlmanacService,
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


class StubAdapter:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        return self.payload


def make_record(dayobs):
    return {
        "dayobs": dayobs,
        "night_hours": 11.0,
        "twilight_evening_12deg": "2024-01-01 23:00:00",
        "twilight_morning_12deg": "2024-01-02 10:00:00",
    }


def make_service(payload):
    adapter = StubAdapter(payload)
    return AlmanacService(almanac_adapter=adapter), adapter


class TestAlmanacService:
    def test_fetch_range_shifted_to_morning_boundary_labels(self):
        service, adapter = make_service({})
        service.handle_request(20250101, 20250103)
        assert adapter.calls == [(20250102, 20250103)]

    def test_records_sorted_ascending_by_dayobs(self):
        payload = {20250103: make_record(20250103), 20250102: make_record(20250102)}
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250103)
        assert [r["dayobs"] for r in response["almanac_info"]] == [20250102, 20250103]

    def test_elapsed_twilight_hours_added_to_each_record(self):
        service, _ = make_service({20240102: make_record(20240102)})
        response = service.handle_request(20240101, 20240102)
        record = response["almanac_info"][0]
        # The 2024 night is long finished, so the full night counts.
        assert record["elapsed_twilight_hours"] == pytest.approx(11.0)

    def test_empty_range_returns_empty_records(self):
        service, _ = make_service({})
        assert service.handle_request(20250101, 20250101) == {"almanac_info": []}
