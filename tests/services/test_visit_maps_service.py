from unittest.mock import patch

import pandas as pd
import pytest

from lsst.ts.logging_and_reporting.services.visit_maps import VisitMapsService
from lsst.ts.logging_and_reporting.services.worker_pool_mixin import WorkerPoolMixin

SERVICE = "lsst.ts.logging_and_reporting.services.visit_maps"


@pytest.fixture(autouse=True)
def run_inline(monkeypatch):
    """Run worker calls in-process so the builders can be patched.

    The real path pickles the callable out to a worker, which a mock
    cannot survive. `WorkerPoolMixin` is covered on its own in
    test_worker_pool_mixin.
    """
    monkeypatch.setattr(WorkerPoolMixin, "run_in_worker", lambda self, func, *args: func(*args))


class StubVisitsAdapter:
    def __init__(self, buckets):
        self.buckets = buckets
        self.fetch_calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.fetch_calls.append((instrument, start_dayobs, end_dayobs))
        return self.buckets


def make_service(buckets=None, consdb_adapter=None):
    return VisitMapsService(consdb_adapter=consdb_adapter or StubVisitsAdapter(buckets or {}))


def visit(day_obs, s_ra=10.0):
    return {"day_obs": day_obs, "s_ra": s_ra, "band": "r"}


class TestHandleRequest:
    def test_fetches_range_with_exclusive_end_converted(self):
        adapter = StubVisitsAdapter({})
        service = make_service(consdb_adapter=adapter)
        service.handle_request(20250101, 20250104, "LSSTCam")
        # dayObsEnd is exclusive, so the inclusive fetch stops at end - 1.
        assert adapter.fetch_calls == [("LSSTCam", 20250101, 20250103)]


class TestCollateResponse:
    def test_empty_range_skips_build(self):
        service = make_service()
        with (
            patch(f"{SERVICE}.rn_aug.augment_visits") as augment,
            patch(f"{SERVICE}.build_visit_maps_using_builder") as build,
        ):
            response = service.collate_response({}, instrument="LSSTCam", applet_mode=False)
        assert response == {"interactive": None}
        augment.assert_not_called()
        build.assert_not_called()

    def test_augments_with_lowercased_instrument_and_builds(self):
        service = make_service()
        buckets = {20250102: [visit(20250102)], 20250101: [visit(20250101)]}
        augmented = pd.DataFrame([{"augmented": True}])
        with (
            patch(f"{SERVICE}.rn_aug.augment_visits", return_value=augmented) as augment,
            patch(f"{SERVICE}.build_visit_maps_using_builder", return_value="figure") as build,
            patch(f"{SERVICE}.json_item", return_value={"root_id": "r"}) as json_item,
        ):
            response = service.collate_response(buckets, instrument="LSSTCam", applet_mode=True)

        assert augment.call_args.kwargs == {"instrument": "lsstcam"}
        assert build.call_args.args[0] is augmented
        assert build.call_args.kwargs == {"applet_mode": True}
        json_item.assert_called_once_with("figure")
        assert response == {"interactive": {"root_id": "r"}}

    def test_no_figure_yields_none(self):
        service = make_service()
        buckets = {20250101: [visit(20250101)]}
        with (
            patch(f"{SERVICE}.rn_aug.augment_visits", return_value=pd.DataFrame([{"a": 1}])),
            patch(f"{SERVICE}.build_visit_maps_using_builder", return_value=None),
            patch(f"{SERVICE}.json_item") as json_item,
        ):
            response = service.collate_response(buckets, instrument="LSSTCam", applet_mode=False)
        assert response == {"interactive": None}
        json_item.assert_not_called()
