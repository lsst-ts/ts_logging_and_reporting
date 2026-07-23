from lsst.ts.logging_and_reporting.services.obs_status import ObsStatusService
from lsst.ts.logging_and_reporting.utils.obs_status import OBSERVATORY_STATES

FAULT = OBSERVATORY_STATES["FAULT"]
ONE_HOUR_MS = 3_600_000


def ev(status, time_ms, note="", labels=None):
    return {"status": status, "time_ms": time_ms, "time": time_ms, "note": note, "statusLabels": labels or []}


class StubObsStatusAdapter:
    def __init__(self, buckets):
        self.buckets = buckets
        self.fetch_calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.fetch_calls.append((start_dayobs, end_dayobs))
        return self.buckets


class StubAlmanacAdapter:
    def __init__(self, records=None):
        self.records = records or {}
        self.fetch_calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.fetch_calls.append((start_dayobs, end_dayobs))
        return self.records


def make_service(buckets=None, almanac=None):
    return ObsStatusService(
        adapters={
            "obs_status": StubObsStatusAdapter(buckets or {}),
            "almanac": StubAlmanacAdapter(almanac),
        }
    )


class TestCollateResponse:
    def test_prepends_carry_in_from_leading_day(self):
        service = make_service()
        buckets = {20250101: [ev(1, 0), ev(2, 1)], 20250102: [ev(3, 2)], 20250103: [ev(4, 3)]}
        # The leading day contributes only its final event (the carry-in).
        assert service.collate_response(buckets) == [ev(2, 1), ev(3, 2), ev(4, 3)]

    def test_no_carry_in_when_leading_day_empty(self):
        service = make_service()
        assert service.collate_response({20250101: [], 20250102: [ev(3, 2)]}) == [ev(3, 2)]

    def test_empty(self):
        assert make_service().collate_response({}) == []


class TestHandleRequest:
    def test_fetches_one_leading_day(self):
        # The events adapter is queried from the day before the range,
        # so the carry-in event before the range start is available.
        service = make_service(buckets={20250101: [], 20250102: []})
        service.handle(20250102, 20250102)
        assert service.adapters["obs_status"].fetch_calls == [(20250101, 20250102)]

    def test_includes_entries_and_availability_by_default(self):
        service = make_service(buckets={20250101: [ev(FAULT, 0)], 20250102: [ev(0, ONE_HOUR_MS)]})
        response = service.handle(20250102, 20250102)
        assert response["entries"] == [ev(FAULT, 0), ev(0, ONE_HOUR_MS)]
        assert response["availability"]["available_from"] == 20260225
        assert "intervals" not in response
        assert "metrics" not in response

    def test_entries_omitted_when_disabled(self):
        service = make_service(buckets={20250101: [], 20250102: []})
        response = service.handle(20250102, 20250102, include_entries=False)
        assert "entries" not in response

    def test_include_intervals(self):
        service = make_service(buckets={20250101: [ev(FAULT, 0)], 20250102: [ev(0, ONE_HOUR_MS)]})
        response = service.handle(20250102, 20250102, include_intervals=True)
        assert len(response["intervals"]) == 1
        assert response["intervals"][0]["start_state"] == FAULT

    def test_night_metric_uses_almanac_offset_by_one(self):
        buckets = {20250101: [ev(FAULT, 0)], 20250102: [ev(0, ONE_HOUR_MS)]}
        almanac = {
            20250103: {
                "twilight_evening_12deg": "1970-01-01 00:00:00",
                "twilight_morning_12deg": "1970-01-01 10:00:00",
            }
        }
        service = ObsStatusService(
            adapters={
                "obs_status": StubObsStatusAdapter(buckets),
                "almanac": StubAlmanacAdapter(almanac),
            }
        )
        response = service.handle(20250102, 20250102, requested_metrics=["fault_loss"])
        assert response["metrics"]["fault_loss"] == 1.0
        # Night windows come from almanac dayobs [start + 1, end + 1].
        assert service.adapters["almanac"].fetch_calls == [(20250103, 20250103)]

    def test_dayobs_metric_skips_almanac(self):
        buckets = {20250101: [ev(FAULT, 0)], 20250102: [ev(0, ONE_HOUR_MS)]}
        service = ObsStatusService(
            adapters={
                "obs_status": StubObsStatusAdapter(buckets),
                "almanac": StubAlmanacAdapter(),
            }
        )
        response = service.handle(
            20250102, 20250102, night_only_metrics=False, requested_metrics=["fault_loss"]
        )
        assert "fault_loss" in response["metrics"]
        assert service.adapters["almanac"].fetch_calls == []

    def test_unknown_metric_ignored(self):
        service = make_service(buckets={20250101: [], 20250102: []})
        response = service.handle(20250102, 20250102, requested_metrics=["bogus"])
        assert response["metrics"] == {}
