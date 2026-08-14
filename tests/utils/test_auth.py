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

from unittest.mock import Mock, patch

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting.utils.auth import (
    AUTH_SOURCES,
    get_access_token,
    get_auth_header,
    get_jira_hostname,
    retrieve_access_token,
)

app = FastAPI()


def test_retrieve_access_token_env(monkeypatch):
    config = AUTH_SOURCES["rsp"]
    monkeypatch.setenv(config["env_var"], "env_token")

    token = retrieve_access_token(config)
    assert token == "env_token"


def test_retrieve_access_token_header(monkeypatch):
    config = AUTH_SOURCES["rsp"]
    monkeypatch.delenv(config["env_var"], raising=False)

    class MockRequest:
        headers = {"Authorization": "Bearer header_token"}

    token = retrieve_access_token(config, request=MockRequest())
    assert token == "header_token"


# RSP notebook: Preferred RSPDiscovery path
def test_retrieve_access_token_rsp_discovery():
    config = AUTH_SOURCES["rsp"]

    # Mock hierarchy
    mock_services = Mock()
    mock_services.RSPDiscovery.get_token.return_value = "rsp-token"

    mock_rsp = Mock()
    mock_rsp._services = mock_services

    mock_lsst = Mock()
    mock_lsst.rsp = mock_rsp

    with patch.dict(
        "sys.modules",
        {
            "lsst": mock_lsst,
            "lsst.rsp": mock_rsp,
            "lsst.rsp._services": mock_services,
        },
    ):
        token = retrieve_access_token(config)
        assert token == "rsp-token"
        mock_services.RSPDiscovery.get_token.assert_called_once()


# RSP notebook: RSPDiscovery fails --> fallback to env var
def test_retrieve_access_token_rsp_fallback_to_env(monkeypatch):
    config = AUTH_SOURCES["rsp"]

    mock_services = Mock()
    mock_services.RSPDiscovery.get_token.side_effect = Exception("no token")

    mock_rsp = Mock()
    mock_rsp._services = mock_services

    mock_lsst = Mock()
    mock_lsst.rsp = mock_rsp

    monkeypatch.setenv("ACCESS_TOKEN", "env_token")

    with patch.dict(
        "sys.modules",
        {
            "lsst": mock_lsst,
            "lsst.rsp": mock_rsp,
            "lsst.rsp._services": mock_services,
        },
    ):
        token = retrieve_access_token(config)
        assert token == "env_token"


# Fallback to deprecated lsst.utils
def test_retrieve_access_token_lsst_utils():
    config = AUTH_SOURCES["rsp"]

    mock_utils = Mock()
    mock_utils.get_access_token.return_value = "lsst-token"

    mock_rsp = Mock()
    mock_rsp.utils = mock_utils

    mock_lsst = Mock()
    mock_lsst.rsp = mock_rsp

    with patch.dict(
        "sys.modules",
        {
            "lsst": mock_lsst,
            "lsst.rsp.utils": mock_utils,
        },
    ):
        token = retrieve_access_token(config)
        assert token == "lsst-token"


# Fetch default (RSP) token via env var
def test_get_access_token_default_env_variable(monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "env_token")
    dependency = get_access_token()
    token = dependency()
    assert token == "env_token"


# Fetch Jira token via env var
def test_get_access_token_jira_env_variable(monkeypatch):
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    dependency = get_access_token("jira")
    token = dependency()
    assert token == "jira-token"


# Fetch Zephyr token via env var
def test_get_access_token_zephyr_env_variable(monkeypatch):
    monkeypatch.setenv("ZEPHYR_API_TOKEN", "zephyr-token")
    dependency = get_access_token("zephyr")
    token = dependency()
    assert token == "zephyr-token"


@app.get("/test-default-access-token")
def access_token_endpoint(
    request: Request = None,
    auth_token: str = Depends(get_access_token()),
):
    return {"token": auth_token}


@app.get("/test-jira-access-token")
def jira_access_token_endpoint(
    request: Request = None,
    auth_token: str = Depends(get_access_token("jira")),
):
    return {"token": auth_token}


@app.get("/test-zephyr-access-token")
def zephyr_access_token_endpoint(
    request: Request = None,
    auth_token: str = Depends(get_access_token("zephyr")),
):
    return {"token": auth_token}


def test_get_access_token_request_headers(monkeypatch):
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-default-access-token", headers={"Authorization": "Bearer header_token"})
    assert response.status_code == 200
    assert response.json() == {"token": "header_token"}


def test_get_access_token_no_rsp_token(monkeypatch):
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-default-access-token")
    assert response.status_code == 401
    assert response.json() == {"detail": "RSP authentication token could not be retrieved by any method."}


def test_get_access_token_no_jira_token(monkeypatch):
    monkeypatch.delenv("JIRA_API_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-jira-access-token")
    assert response.status_code == 401
    assert response.json() == {"detail": "Jira authentication token could not be retrieved by any method."}


def test_get_access_token_no_zephyr_token(monkeypatch):
    monkeypatch.delenv("ZEPHYR_API_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-zephyr-access-token")
    assert response.status_code == 401
    assert response.json() == {"detail": "Zephyr authentication token could not be retrieved by any method."}


def test_get_auth_header_valid():
    token = "my-token"
    header = get_auth_header(token)
    assert header == {"Authorization": f"Bearer {token}"}


def test_get_auth_header_none():
    try:
        get_auth_header(None)
    except ValueError as e:
        assert str(e) == "Auth token is required"
    else:
        assert False, "Expected ValueError"


def test_get_auth_header_empty():
    try:
        get_auth_header("")
    except ValueError as e:
        assert str(e) == "Auth token is required"
    else:
        assert False, "Expected ValueError"


def test_get_jira_hostname_env(monkeypatch):
    monkeypatch.setenv("JIRA_API_HOSTNAME", "jira.example.com")
    hostname = get_jira_hostname()
    assert hostname == "jira.example.com"


def test_get_jira_hostname_missing(monkeypatch):
    monkeypatch.delenv("JIRA_API_HOSTNAME", raising=False)
    try:
        get_jira_hostname()
    except HTTPException as e:
        assert e.status_code == 500
        assert e.detail == "Jira hostname not configured"
    else:
        assert False, "Expected HTTPException"
