from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.utils.auth import (
    AUTH_SOURCES,
    get_auth_header,
    get_jira_hostname,
    retrieve_access_token,
)


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


# Fetch the Jira token via env var
def test_retrieve_access_token_jira_env(monkeypatch):
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    assert retrieve_access_token(AUTH_SOURCES["jira"]) == "jira-token"


# Fetch the Zephyr token via env var
def test_retrieve_access_token_zephyr_env(monkeypatch):
    monkeypatch.setenv("ZEPHYR_API_TOKEN", "zephyr-token")
    assert retrieve_access_token(AUTH_SOURCES["zephyr"]) == "zephyr-token"


@pytest.mark.parametrize(
    "source, env_var, label",
    [
        ("rsp", "ACCESS_TOKEN", "RSP"),
        ("jira", "JIRA_API_TOKEN", "Jira"),
        ("zephyr", "ZEPHYR_API_TOKEN", "Zephyr"),
    ],
)
def test_retrieve_access_token_missing(monkeypatch, source, env_var, label):
    monkeypatch.delenv(env_var, raising=False)
    with pytest.raises(HTTPException) as excinfo:
        retrieve_access_token(AUTH_SOURCES[source])
    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == f"{label} authentication token could not be retrieved by any method."


def test_get_auth_header_valid():
    token = "my-token"
    header = get_auth_header(token)
    assert header == {"Authorization": f"Bearer {token}"}


@pytest.mark.parametrize("token", [None, ""])
def test_get_auth_header_missing_token(token):
    with pytest.raises(ValueError, match="Auth token is required"):
        get_auth_header(token)


def test_get_jira_hostname_env(monkeypatch):
    monkeypatch.setenv("JIRA_API_HOSTNAME", "jira.example.com")
    hostname = get_jira_hostname()
    assert hostname == "jira.example.com"


def test_get_jira_hostname_missing(monkeypatch):
    monkeypatch.delenv("JIRA_API_HOSTNAME", raising=False)
    with pytest.raises(HTTPException) as excinfo:
        get_jira_hostname()
    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == "Jira hostname not configured"
