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

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting.web_app.middleware.cache_control import (
    _ALWAYS_SHORT_PATHS,
    _HISTORICAL_MAX_AGE,
    _TODAY_MAX_AGE,
    CacheControlMiddleware,
)

MOCK_TODAY = 20260101


def _make_app():
    """Create a minimal FastAPI app with the cache middleware."""
    app = FastAPI()
    app.add_middleware(CacheControlMiddleware)

    @app.get("/some-endpoint")
    def some_endpoint():
        return {"ok": True}

    @app.get("/exposure-flags")
    def exposure_flags():
        return {"ok": True}

    @app.get("/block-details")
    def block_details():
        return {"ok": True}

    @app.get("/exposure-entries")
    def exposure_entries():
        return {"ok": True}

    @app.get("/narrative-log")
    def narrative_log():
        return {"ok": True}

    @app.get("/health")
    def health():
        return {"ok": True}

    return app


@pytest.fixture()
def client():
    return TestClient(_make_app())


def _cache_header(response):
    return response.headers.get("Cache-Control")


# -- dayobs-based caching --


@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_historical_range_gets_long_ttl(mock_today, client):
    resp = client.get("/some-endpoint", params={"dayObsStart": 20250101, "dayObsEnd": 20250131})
    assert _cache_header(resp) == f"public, max-age={_HISTORICAL_MAX_AGE}"


@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_range_including_today_gets_short_ttl(mock_today, client):
    resp = client.get(
        "/some-endpoint",
        params={"dayObsStart": 20251201, "dayObsEnd": 20260201},
    )
    assert _cache_header(resp) == f"public, max-age={_TODAY_MAX_AGE}"


@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_single_dayobs_today_gets_short_ttl(mock_today, client):
    resp = client.get("/some-endpoint", params={"dayObs": MOCK_TODAY})
    assert _cache_header(resp) == f"public, max-age={_TODAY_MAX_AGE}"


@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_single_dayobs_historical_gets_long_ttl(mock_today, client):
    resp = client.get("/some-endpoint", params={"dayObs": 20250601})
    assert _cache_header(resp) == f"public, max-age={_HISTORICAL_MAX_AGE}"


# -- no dayobs params → no header --


def test_no_dayobs_params_no_cache_header(client):
    resp = client.get("/health")
    assert _cache_header(resp) is None


# -- always-short-path endpoints --


@pytest.mark.parametrize("path", sorted(_ALWAYS_SHORT_PATHS))
@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_always_short_paths_get_short_ttl(mock_today, path, client):
    resp = client.get(path, params={"dayObsStart": 20250101, "dayObsEnd": 20250131})
    assert _cache_header(resp) == f"public, max-age={_TODAY_MAX_AGE}"


@pytest.mark.parametrize("path", sorted(_ALWAYS_SHORT_PATHS))
def test_always_short_paths_without_dayobs(path, client):
    resp = client.get(path)
    assert _cache_header(resp) == f"public, max-age={_TODAY_MAX_AGE}"


# -- edge cases --


def test_invalid_dayobs_no_cache_header(client):
    resp = client.get("/some-endpoint", params={"dayObs": "not-a-number"})
    assert _cache_header(resp) is None


@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_only_start_param(mock_today, client):
    resp = client.get("/some-endpoint", params={"dayObsStart": 20250601})
    assert _cache_header(resp) == f"public, max-age={_HISTORICAL_MAX_AGE}"


@patch(
    "lsst.ts.logging_and_reporting.web_app.middleware.cache_control._today_dayobs",
    return_value=MOCK_TODAY,
)
def test_only_end_param(mock_today, client):
    resp = client.get("/some-endpoint", params={"dayObsEnd": MOCK_TODAY})
    assert _cache_header(resp) == f"public, max-age={_TODAY_MAX_AGE}"
