from lsst.ts.logging_and_reporting.services.exposure_flags import (
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


def make_service(payload):
    adapter = StubAdapter(payload)
    return ExposureFlagsService(exposurelog_adapter=adapter), adapter


class TestExposureFlagsService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service({})
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
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert {f["obs_id"] for f in response["exposure_flags"]} == {"junk-obs", "questionable-obs"}

    def test_response_shape_is_obs_id_and_flag_only(self):
        payload = {20250101: [make_message(20250101, obs_id="obs-9", flag="junk")]}
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert response["exposure_flags"] == [{"obs_id": "obs-9", "exposure_flag": "junk"}]
