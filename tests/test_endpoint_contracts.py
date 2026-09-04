# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""Contract tests for the API endpoints.

Each endpoint's job is uniform: declare its query params, forward them
to ``service.handle_request``, and return the result. These tests cover exactly
that -- param parsing, forwarding, pass-through, and status mapping --
by overriding the service with a stub. Response payloads and collation
are covered in ``tests/services``; upstream parsing and caching in
``tests/adapters``.
"""

import re

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

    def handle_request(self, *args, **kwargs):
        return self.result


class CapturingService:
    """Records the positional args ``handle_request`` is called with."""

    def __init__(self):
        self.calls = []

    def handle_request(self, *args):
        self.calls.append(args)
        return SENTINEL


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


# (service getter, request URL, args the endpoint should forward to handle).
FORWARDING = [
    (
        web_services.get_almanac_service,
        "/almanac?dayObsStart=20240101&dayObsEnd=20240102",
        (20240101, 20240102),
    ),
    (
        web_services.get_data_log_service,
        "/data-log?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam",
        (20240101, 20240102, "LSSTCam"),
    ),
]

FORWARDING_IDS = [
    "almanac",
    "data-log",
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

