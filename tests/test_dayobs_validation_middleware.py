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

import logging

import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting.middleware.dayobs_validation import (
    DayobsValidationMiddleware,
    _validate_dayobs_range,
)


class TestValidateDayobsRange:
    """Unit tests for the pure validation function, independent of ASGI."""

    def test_valid_range_passes(self):
        assert _validate_dayobs_range("20250101", "20250102") is None

    def test_equal_bounds_pass(self):
        assert _validate_dayobs_range("20250101", "20250101") is None

    def test_malformed_start_returns_error(self, caplog):
        with caplog.at_level(logging.WARNING):
            error = _validate_dayobs_range("20250230", "20250301")
        assert error == "Invalid dayObsStart: 20250230"
        assert "Rejected malformed dayObsStart: 20250230" in caplog.text

    def test_malformed_end_returns_error(self, caplog):
        with caplog.at_level(logging.WARNING):
            error = _validate_dayobs_range("20250101", "99999999")
        assert error == "Invalid dayObsEnd: 99999999"
        assert "Rejected malformed dayObsEnd: 99999999" in caplog.text

    def test_inverted_range_returns_error(self, caplog):
        with caplog.at_level(logging.WARNING):
            error = _validate_dayobs_range("20250102", "20250101")
        assert error == "dayObsStart (20250102) must not be after dayObsEnd (20250101)"
        assert "Rejected inverted dayobs range" in caplog.text

    def test_malformed_start_checked_before_range_order(self):
        # A malformed start should surface as the date error, not the
        # (also true) inverted-range error.
        error = _validate_dayobs_range("20250230", "20250101")
        assert error == "Invalid dayObsStart: 20250230"

    def test_non_numeric_values_are_left_to_fastapi(self):
        assert _validate_dayobs_range("abc", "20250101") is None
        assert _validate_dayobs_range("20250101", "abc") is None


def _make_app():
    """Create a minimal FastAPI app with the dayobs validation middleware.

    Middleware order matters: DayobsValidationMiddleware is added first
    (innermost) so CORSMiddleware still gets to process a short-circuited
    422 response, matching how they're wired in main.py.
    """
    app = FastAPI()
    app.add_middleware(DayobsValidationMiddleware)
    app.add_middleware(CORSMiddleware, allow_origins=["http://example.test"])

    @app.get("/some-endpoint")
    def some_endpoint():
        return {"ok": True}

    @app.get("/typed-endpoint")
    def typed_endpoint(dayObsStart: int, dayObsEnd: int):
        return {"dayObsStart": dayObsStart, "dayObsEnd": dayObsEnd}

    return app


@pytest.fixture()
def client():
    return TestClient(_make_app())


class TestMiddlewareIntegration:
    def test_valid_range_passes_through(self, client):
        resp = client.get("/some-endpoint", params={"dayObsStart": 20250101, "dayObsEnd": 20250102})
        assert resp.status_code == 200

    def test_malformed_start_returns_422(self, client):
        resp = client.get("/some-endpoint", params={"dayObsStart": 20250230, "dayObsEnd": 20250301})
        assert resp.status_code == 422
        assert resp.json() == {"detail": "Invalid dayObsStart: 20250230"}

    def test_inverted_range_returns_422(self, client):
        resp = client.get("/some-endpoint", params={"dayObsStart": 20250102, "dayObsEnd": 20250101})
        assert resp.status_code == 422

    def test_missing_one_param_passes_through_untouched(self, client):
        resp = client.get("/some-endpoint", params={"dayObsStart": 20250101})
        assert resp.status_code == 200

    def test_non_numeric_value_falls_through_to_fastapi_422(self, client):
        # The middleware doesn't reject "abc" itself; FastAPI's own
        # query-param type coercion does, once it reaches the endpoint.
        resp = client.get("/typed-endpoint", params={"dayObsStart": "abc", "dayObsEnd": 20250101})
        assert resp.status_code == 422

    def test_unmatched_route_still_returns_404(self, client):
        # A malformed range on a path that doesn't exist at all must not
        # be shadowed by our 422 -- the router's own 404 should win.
        resp = client.get("/no-such-route", params={"dayObsStart": 20250230, "dayObsEnd": 20250101})
        assert resp.status_code == 404

    def test_wrong_method_on_real_path_still_returns_405(self, client):
        # Path exists but only supports GET; a malformed range must not
        # be shadowed by our 422 -- the router's own 405 should win.
        resp = client.post("/some-endpoint", params={"dayObsStart": 20250230, "dayObsEnd": 20250101})
        assert resp.status_code == 405

    def test_short_circuited_422_still_gets_cors_headers(self, client):
        # Regression test for middleware ordering: DayobsValidationMiddleware
        # must be wrapped by CORSMiddleware, not the other way around, or a
        # short-circuited response never reaches CORSMiddleware at all.
        resp = client.get(
            "/some-endpoint",
            params={"dayObsStart": 20250102, "dayObsEnd": 20250101},
            headers={"Origin": "http://example.test"},
        )
        assert resp.status_code == 422
        assert resp.headers.get("access-control-allow-origin") == "http://example.test"
