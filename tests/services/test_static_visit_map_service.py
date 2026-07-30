from unittest.mock import patch

import pandas as pd
from matplotlib import pyplot as plt

from lsst.ts.logging_and_reporting.services import static_visit_map
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
        service.handle_request(20250101, 20250104, "LSSTCam")
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


def test_compute_nvisits_bundle_uses_static_map_plot_config(monkeypatch):
    captured = {}

    class DummyMetricBundle:
        def __init__(self, metric, slicer, constraint, plot_funcs=None, plot_dict=None):
            captured["plot_dict"] = plot_dict

    class DummyMetricBundleGroup:
        def __init__(self, bundles, db_obj, save_early=False):
            self.bundles = bundles

        def run_current(self, constraint, map_data):
            captured["run_current_args"] = (constraint, map_data)

    monkeypatch.setattr(static_visit_map.maf, "MetricBundle", DummyMetricBundle)
    monkeypatch.setattr(static_visit_map.maf, "MetricBundleGroup", DummyMetricBundleGroup)

    map_data = [{"s_ra": 10.0, "s_dec": -20.0, "sky_rotation": 45.0, "obs_start_mjd": 60000.0}]
    static_visit_map._compute_nvisits_bundle(map_data)

    assert captured["plot_dict"]["title"] == ""
    assert captured["plot_dict"]["bgcolor"] == static_visit_map.COLOR_BG
    assert captured["plot_dict"]["badcolor"] == static_visit_map.COLOR_BG
    assert captured["run_current_args"] == ("", map_data)


def test_build_static_visit_map_styles_and_adds_graticules(monkeypatch):
    fig, ax = plt.subplots()
    ax.imshow([[1, 2], [3, 4]])

    style_calls = []
    graticule_calls = []

    class DummyBundle:
        def plot(self):
            return {"SkyMap": fig.number}

    def fake_style_figure(styled_fig, main_ax):
        style_calls.append((styled_fig, main_ax))

    def fake_add_graticules(main_ax):
        graticule_calls.append(main_ax)

    monkeypatch.setattr(static_visit_map, "_compute_nvisits_bundle", lambda map_data: DummyBundle())
    monkeypatch.setattr(static_visit_map, "_style_figure", fake_style_figure)
    monkeypatch.setattr(static_visit_map, "_add_graticules", fake_add_graticules)

    try:
        visits = pd.DataFrame(
            [
                {
                    "s_ra": 10.0,
                    "s_dec": -20.0,
                    "sky_rotation": 45.0,
                    "obs_start_mjd": 60000.0,
                    "science_program": "BLOCK-365",
                }
            ]
        )
        png_bytes = static_visit_map.build_static_visit_map(visits)
    finally:
        plt.close(fig)

    assert png_bytes
    assert style_calls == [(fig, ax)]
    assert graticule_calls == [ax]
