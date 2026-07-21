from lsst.ts.logging_and_reporting.services.exposure_entries import (
    ExposureEntriesService,
)


class StubAdapter:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        return self.payload


def make_message(day_obs, instrument="LSSTCam", flag="junk", obs_id="obs-1", date_added=""):
    return {
        "obs_id": obs_id,
        "instrument": instrument,
        "day_obs": day_obs,
        "exposure_flag": flag,
        "date_added": date_added or f"{day_obs}T00:00:00",
    }


def make_service(payload):
    adapter = StubAdapter(payload)
    return ExposureEntriesService(adapters={"exposurelog": adapter}), adapter


class TestExposureEntriesService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service({})
        service.handle_request(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [(20250101, 20250102)]

    def test_filters_by_instrument(self):
        payload = {
            20250101: [
                make_message(20250101, instrument="LSSTCam"),
                make_message(20250101, instrument="LATISS"),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert len(response["exposure_entries"]) == 1
        assert response["exposure_entries"][0]["instrument"] == "LSSTCam"

    def test_entries_sorted_newest_first_across_days(self):
        payload = {
            20250101: [make_message(20250101, obs_id="older", date_added="2025-01-01T10:00:00")],
            20250102: [make_message(20250102, obs_id="newer", date_added="2025-01-02T10:00:00")],
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250103, "LSSTCam")
        assert [e["obs_id"] for e in response["exposure_entries"]] == ["newer", "older"]
