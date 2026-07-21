from lsst.ts.logging_and_reporting.web_app.services.exposure_entries import (
    ExposureEntriesService,
)
from lsst.ts.logging_and_reporting.web_app.services.exposure_flags import (
    ExposureFlagsService,
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


def make_service(service_class, payload):
    adapter = StubAdapter(payload)
    return service_class(adapters={"exposurelog": adapter}), adapter


class TestExposureEntriesService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service(ExposureEntriesService, {})
        service.handle_request(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [(20250101, 20250102)]

    def test_filters_by_instrument(self):
        payload = {
            20250101: [
                make_message(20250101, instrument="LSSTCam"),
                make_message(20250101, instrument="LATISS"),
            ]
        }
        service, _ = make_service(ExposureEntriesService, payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert len(response["exposure_entries"]) == 1
        assert response["exposure_entries"][0]["instrument"] == "LSSTCam"

    def test_entries_sorted_newest_first_across_days(self):
        payload = {
            20250101: [make_message(20250101, obs_id="older", date_added="2025-01-01T10:00:00")],
            20250102: [make_message(20250102, obs_id="newer", date_added="2025-01-02T10:00:00")],
        }
        service, _ = make_service(ExposureEntriesService, payload)
        response = service.handle_request(20250101, 20250103, "LSSTCam")
        assert [e["obs_id"] for e in response["exposure_entries"]] == ["newer", "older"]


class TestExposureFlagsService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service(ExposureFlagsService, {})
        service.handle_request(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [(20250101, 20250102)]

    def test_filters_flag_values_and_instrument(self):
        payload = {
            20250101: [
                make_message(20250101, obs_id="junk-obs", flag="junk"),
                make_message(20250101, obs_id="questionable-obs", flag="questionable"),
                make_message(20250101, obs_id="unknown-obs", flag="unknown"),
                make_message(20250101, obs_id="no-flag-obs", flag=None),
                make_message(20250101, obs_id="other-cam", flag="junk", instrument="LATISS"),
            ]
        }
        service, _ = make_service(ExposureFlagsService, payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert {f["obs_id"] for f in response["exposure_flags"]} == {"junk-obs", "questionable-obs"}

    def test_response_shape_is_obs_id_and_flag_only(self):
        payload = {20250101: [make_message(20250101, obs_id="obs-9", flag="junk")]}
        service, _ = make_service(ExposureFlagsService, payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert response["exposure_flags"] == [{"obs_id": "obs-9", "exposure_flag": "junk"}]
