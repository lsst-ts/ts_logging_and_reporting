#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Contract tests for the API endpoints.

Each endpoint's job is uniform: declare its query params, forward them
to ``service.handle``, and return the result. These tests cover exactly
that -- param parsing, forwarding, pass-through, and status mapping --
by overriding the service with a stub. Response payloads and collation
are covered in ``tests/services``; upstream parsing and caching in
``tests/adapters``.
"""

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting import __version__
from lsst.ts.logging_and_reporting import services as web_services
from lsst.ts.logging_and_reporting.main import app

client = TestClient(app)

SENTINEL = {"result": "ok", "items": [1, 2, 3]}


class StubService:
    """Returns a canned response, for pass-through tests."""

    def __init__(self, result):
        self.result = result

    def handle(self, *args, **kwargs):
        return self.result


class CapturingService:
    """Records the positional args ``handle`` is called with."""

    def __init__(self):
        self.calls = []

    def handle(self, *args):
        self.calls.append(args)
        return SENTINEL


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


# (service getter, request URL, args the endpoint should forward to handle).
FORWARDING = [
    (
        web_services.get_exposures_service,
        "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam",
        (20240101, 20240102, "LSSTCam"),
    ),
    (
        web_services.get_expected_exposures_service,
        "/expected-exposures?dayObsStart=20240101&dayObsEnd=20240102",
        (20240101, 20240102),
    ),
    (
        web_services.get_data_log_service,
        "/data-log?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam",
        (20240101, 20240102, "LSSTCam"),
    ),
    (
        web_services.get_jira_tickets_service,
        "/jira-tickets?dayObsStart=20250730&dayObsEnd=20250731&instrument=LATISS",
        (20250730, 20250731, "LATISS"),
    ),
    (
        web_services.get_almanac_service,
        "/almanac?dayObsStart=20240101&dayObsEnd=20240102",
        (20240101, 20240102),
    ),
    (
        web_services.get_narrative_log_service,
        "/narrative-log?dayObsStart=20250730&dayObsEnd=20250731&instrument=LSSTCam",
        (20250730, 20250731, "LSSTCam"),
    ),
    (
        web_services.get_exposure_flags_service,
        "/exposure-flags?dayObsStart=20250730&dayObsEnd=20250731&instrument=LSSTCam",
        (20250730, 20250731, "LSSTCam"),
    ),
    (
        web_services.get_exposure_entries_service,
        "/exposure-entries?dayObsStart=20250730&dayObsEnd=20250731&instrument=LSSTCam",
        (20250730, 20250731, "LSSTCam"),
    ),
    (
        web_services.get_night_report_service,
        "/night-reports?dayObsStart=20250730&dayObsEnd=20250731",
        (20250730, 20250731),
    ),
    (
        web_services.get_context_feed_service,
        "/context-feed?dayObsStart=20240101&dayObsEnd=20240102",
        (20240101, 20240102),
    ),
    (
        web_services.get_obs_status_service,
        "/obs-status?dayObsStart=20250101&dayObsEnd=20250102",
        (20250101, 20250102, True, False, True, None),
    ),
    (
        web_services.get_visit_maps_service,
        "/multi-night-visit-maps?dayObsStart=20240101&dayObsEnd=20240104&instrument=latiss",
        (20240101, 20240104, "latiss", False),
    ),
    (
        web_services.get_static_visit_map_service,
        "/static-visit-map?dayObsStart=20240101&dayObsEnd=20240102&instrument=lsstCam",
        (20240101, 20240102, "lsstCam"),
    ),
    (
        web_services.get_block_details_service,
        "/block-details?key=BLOCK-1",
        (["BLOCK-1"],),
    ),
]

FORWARDING_IDS = [
    "exposures",
    "expected-exposures",
    "data-log",
    "jira-tickets",
    "almanac",
    "narrative-log",
    "exposure-flags",
    "exposure-entries",
    "night-reports",
    "context-feed",
    "obs-status",
    "multi-night-visit-maps",
    "static-visit-map",
    "block-details",
]


@pytest.mark.parametrize(("getter", "url", "expected"), FORWARDING, ids=FORWARDING_IDS)
def test_returns_service_response(getter, url, expected):
    app.dependency_overrides[getter] = lambda: StubService(SENTINEL)

    response = client.get(url)

    assert response.status_code == 200
    assert response.json() == SENTINEL


@pytest.mark.parametrize(("getter", "url", "expected"), FORWARDING, ids=FORWARDING_IDS)
def test_forwards_parsed_params(getter, url, expected):
    service = CapturingService()
    app.dependency_overrides[getter] = lambda: service

    response = client.get(url)

    assert response.status_code == 200
    assert service.calls == [expected]


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_version():
    response = client.get("/version")
    assert response.status_code == 200
    assert response.json()["version"] == __version__


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("false", False),
        ("1", True),
        ("0", False),
        ("yes", True),
        ("no", False),
        ("on", True),
        ("off", False),
    ],
)
def test_obs_status_bool_coercion(value, expected):
    service = CapturingService()
    app.dependency_overrides[web_services.get_obs_status_service] = lambda: service

    client.get(f"/obs-status?dayObsStart=20250101&dayObsEnd=20250102&includeEntries={value}")

    assert service.calls[0][2] is expected


def test_obs_status_forwards_all_params():
    service = CapturingService()
    app.dependency_overrides[web_services.get_obs_status_service] = lambda: service

    client.get(
        "/obs-status?dayObsStart=20250101&dayObsEnd=20250102"
        "&includeEntries=false&includeIntervals=true&nightOnlyMetrics=false"
        "&metric=fault_loss&metric=weather"
    )

    assert service.calls == [(20250101, 20250102, False, True, False, ["fault_loss", "weather"])]


def test_visit_maps_forwards_applet_mode():
    service = CapturingService()
    app.dependency_overrides[web_services.get_visit_maps_service] = lambda: service

    client.get(
        "/multi-night-visit-maps?dayObsStart=20240101&dayObsEnd=20240104&instrument=latiss&appletMode=true"
    )

    assert service.calls == [(20240101, 20240104, "latiss", True)]


def test_block_details_forwards_all_keys():
    service = CapturingService()
    app.dependency_overrides[web_services.get_block_details_service] = lambda: service

    client.get("/block-details?key=BLOCK-1&key=BLOCK-2")

    assert service.calls == [(["BLOCK-1", "BLOCK-2"],)]


@pytest.mark.parametrize(
    "url",
    [
        "/exposures?dayObsEnd=20240102&instrument=LSSTCam",
        "/exposures?dayObsStart=20240101&instrument=LSSTCam",
        "/exposures?dayObsStart=20240101&dayObsEnd=20240102",
        "/exposures?dayObsStart=abc&dayObsEnd=20240102&instrument=LSSTCam",
        "/block-details",
    ],
    ids=["missing-start", "missing-end", "missing-instrument", "non-integer", "missing-key"],
)
def test_missing_or_invalid_params_return_422(url):
    response = client.get(url)
    assert response.status_code == 422


@pytest.mark.parametrize("status", [404, 422, 502])
def test_service_httpexception_surfaces_as_status(status):
    class Raising:
        def handle(self, *args):
            raise HTTPException(status_code=status, detail="boom")

    app.dependency_overrides[web_services.get_obs_status_service] = lambda: Raising()

    response = client.get("/obs-status?dayObsStart=20250101&dayObsEnd=20250102")

    assert response.status_code == status
