from unittest.mock import patch

from lsst.ts.logging_and_reporting.services.static_visit_map import StaticVisitMapService

SERVICE = "lsst.ts.logging_and_reporting.services.static_visit_map"


class StubVisitsAdapter:
    def __init__(self, buckets):
        self.buckets = buckets
        self.fetch_calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.fetch_calls.append((instrument, start_dayobs, end_dayobs))
        return self.buckets


def make_service(buckets=None):
    return StaticVisitMapService(adapters={"consdb": StubVisitsAdapter(buckets or {})})


def visit(day_obs):
    return {"day_obs": day_obs, "s_ra": 10.0, "science_program": "BLOCK-365"}


class TestHandleRequest:
    def test_fetches_range_with_exclusive_end_converted(self):
        service = make_service()
        service.handle(20250101, 20250104, "LSSTCam")
        # dayObsEnd is exclusive, so the inclusive fetch stops at end - 1.
        assert service.adapters["consdb"].fetch_calls == [("LSSTCam", 20250101, 20250103)]


class TestCollateResponse:
    def test_empty_range_skips_build(self):
        service = make_service()
        with patch(f"{SERVICE}.build_static_visit_map") as build:
            response = service.collate_response({})
        assert response == {"static_map": None}
        build.assert_not_called()

    def test_builds_and_encodes_png(self):
        service = make_service()
        buckets = {20250102: [visit(20250102)], 20250101: [visit(20250101)]}
        with patch(f"{SERVICE}.build_static_visit_map", return_value=b"png") as build:
            response = service.collate_response(buckets)

        assert build.call_count == 1
        # The two nights flatten into one frame passed to the builder.
        assert len(build.call_args.args[0]) == 2
        assert response["static_map"]["mime_type"] == "image/png"
        assert response["static_map"]["data"]

    def test_no_png_yields_none(self):
        service = make_service()
        buckets = {20250101: [visit(20250101)]}
        with patch(f"{SERVICE}.build_static_visit_map", return_value=None):
            response = service.collate_response(buckets)
        assert response == {"static_map": None}
