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
from unittest.mock import patch

import pytest
from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting.middleware import request_logging
from lsst.ts.logging_and_reporting.middleware.request_logging import (
    RequestLoggingMiddleware,
)
from lsst.ts.logging_and_reporting.utils.logging_config import (
    NO_REQUEST_ID,
    current_request_id,
)


def _make_app():
    """Create a minimal FastAPI app with the request logging middleware."""
    app = FastAPI()
    app.add_middleware(RequestLoggingMiddleware)

    @app.get("/exposures")
    def exposures(dayObsStart: int = 0, dayObsEnd: int = 0, instrument: str = ""):
        return {"ok": True}

    @app.get("/version")
    def version():
        return {"version": "1.2.3"}

    @app.get("/health")
    @app.get("/health/")
    def health():
        # Reports the ID so the tests can show an unlogged path is
        # never given one.
        return {"status": "ok", "request_id": current_request_id()}

    @app.get("/echo-id")
    def echo_id():
        # Reads the ID from inside the endpoint, which is what proves
        # the middleware's context reached the handler.
        return {"request_id": current_request_id()}

    @app.get("/not-found")
    def not_found():
        return Response(status_code=404)

    @app.get("/boom")
    def boom():
        raise RuntimeError("endpoint exploded")

    return app


@pytest.fixture()
def client():
    return TestClient(_make_app())


@pytest.fixture(autouse=True)
def capture_debug(caplog):
    caplog.set_level(logging.DEBUG, logger=request_logging.__name__)


def messages_at(caplog, level):
    """The middleware's log lines at ``level``, in order."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == request_logging.__name__ and record.levelno == level
    ]


class TestArrivalLine:
    def test_path_and_query_are_logged_at_info(self, client, caplog):
        client.get(
            "/exposures",
            params={
                "dayObsStart": 20250101,
                "dayObsEnd": 20250102,
                "instrument": "LSSTCam",
            },
        )
        assert messages_at(caplog, logging.INFO) == [
            "Fetching /exposures with "
            "dayObsStart=20250101&dayObsEnd=20250102&instrument=LSSTCam"
        ]

    def test_request_without_query_says_so(self, client, caplog):
        client.get("/version")
        assert messages_at(caplog, logging.INFO) == [
            "Fetching /version with no parameters"
        ]

    def test_arrival_is_logged_before_the_response(self, client, caplog):
        client.get("/exposures")
        # The INFO line lands first, so a request that never returns is
        # still visible in the log.
        assert caplog.records[0].levelno == logging.INFO
        assert caplog.records[0].getMessage().startswith("Fetching /exposures")


class TestUnloggedPaths:
    def test_health_is_not_logged(self, client, caplog):
        client.get("/health")
        assert messages_at(caplog, logging.INFO) == []
        assert messages_at(caplog, logging.DEBUG) == []

    def test_trailing_slash_health_is_not_logged(self, client, caplog):
        client.get("/health/")
        assert caplog.records == []

    def test_version_is_logged(self, client, caplog):
        # /version is not a probe endpoint, so it is logged normally.
        client.get("/version")
        assert len(messages_at(caplog, logging.INFO)) == 1


class TestRequestId:
    def test_the_endpoint_sees_an_id(self, client):
        request_id = client.get("/echo-id").json()["request_id"]
        assert request_id != NO_REQUEST_ID
        assert len(request_id) == 8

    def test_each_request_gets_its_own_id(self, client):
        first = client.get("/echo-id").json()["request_id"]
        second = client.get("/echo-id").json()["request_id"]
        assert first != second

    def test_unlogged_paths_are_not_given_an_id(self, client):
        assert client.get("/health").json()["request_id"] == NO_REQUEST_ID

    def test_the_id_does_not_leak_out_of_the_request(self, client):
        client.get("/echo-id")
        assert current_request_id() == NO_REQUEST_ID


class TestCompletionLine:
    def test_status_and_duration_at_debug(self, client, caplog):
        client.get("/exposures")
        debug = messages_at(caplog, logging.DEBUG)
        assert len(debug) == 1
        assert debug[0].startswith("/exposures responded 200 in ")
        assert debug[0].endswith("s")

    def test_error_status_is_reported(self, client, caplog):
        client.get("/not-found")
        assert messages_at(caplog, logging.DEBUG)[0].startswith(
            "/not-found responded 404 in "
        )

    def test_no_completion_line_when_debug_is_off(self, client, caplog):
        caplog.set_level(logging.INFO, logger=request_logging.__name__)
        client.get("/exposures")
        assert messages_at(caplog, logging.DEBUG) == []
        assert len(messages_at(caplog, logging.INFO)) == 1


class TestSlowRequests:
    @patch.object(request_logging, "SLOW_REQUEST_SECONDS", 0)
    def test_slow_request_warns_with_the_query(self, client, caplog):
        client.get("/exposures", params={"dayObsStart": 20250101})
        warnings = messages_at(caplog, logging.WARNING)
        assert len(warnings) == 1
        assert warnings[0].startswith(
            "Slow request: /exposures with dayObsStart=20250101 responded 200 in "
        )

    @patch.object(request_logging, "SLOW_REQUEST_SECONDS", 0)
    def test_slow_request_does_not_also_log_at_debug(self, client, caplog):
        client.get("/exposures")
        assert messages_at(caplog, logging.DEBUG) == []

    def test_fast_request_does_not_warn(self, client, caplog):
        client.get("/exposures")
        assert messages_at(caplog, logging.WARNING) == []


class TestFailedRequests:
    def test_exception_is_logged_and_propagates(self, client, caplog):
        with pytest.raises(RuntimeError):
            client.get("/boom")
        warnings = messages_at(caplog, logging.WARNING)
        assert len(warnings) == 1
        assert warnings[0].startswith("Request /boom with no parameters failed after ")

    def test_failure_still_logs_the_arrival_line(self, client, caplog):
        with pytest.raises(RuntimeError):
            client.get("/boom")
        assert messages_at(caplog, logging.INFO) == [
            "Fetching /boom with no parameters"
        ]
