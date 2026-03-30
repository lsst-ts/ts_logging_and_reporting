import json
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting.utils import (
    get_access_token,
    get_auth_header,
    get_jira_hostname,
    make_json_safe,
    stringify_special_floats,
)

app = FastAPI()


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


# RSP notebook: Preferred RSPDiscovery path
def test_get_access_token_rsp_discovery():
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
        dependency = get_access_token()
        token = dependency()
        assert token == "rsp-token"
        mock_services.RSPDiscovery.get_token.assert_called_once()


# RSP notebook: RSPDiscovery fails --> fallback to env var
def test_get_access_token_rsp_fallback_to_env(monkeypatch):
    # Mock hierarchy
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
        dependency = get_access_token()
        token = dependency()
        assert token == "env_token"


# Fallback to deprecated lsst.utils
def test_get_access_token_lsst_utils():
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
        dependency = get_access_token()
        token = dependency()
        assert token == "lsst-token"


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


def test_stringify_special_floats_nan():
    assert stringify_special_floats(np.nan) == "NaN"


def test_stringify_special_floats_pos_inf():
    assert stringify_special_floats(np.inf) == "Infinity"


def test_stringify_special_floats_neg_inf():
    assert stringify_special_floats(-np.inf) == "-Infinity"


def test_stringify_special_floats_regular_float():
    assert stringify_special_floats(42.5) == 42.5


def test_stringify_special_floats_non_float_type():
    assert stringify_special_floats("hello") == "hello"
    assert stringify_special_floats(123) == 123


# Basic types
def test_make_json_safe_basic_types():
    assert make_json_safe(None) is None
    assert make_json_safe(True) is True
    assert make_json_safe("hello") == "hello"
    assert make_json_safe(42) == 42
    assert make_json_safe(3.14) == 3.14


# Special floats
def test_make_json_safe_nan_and_inf():
    assert make_json_safe(float("nan")) is None
    assert make_json_safe(float("inf")) is None
    assert make_json_safe(np.nan) is None
    assert make_json_safe(np.inf) is None


# NumPy types
def test_make_json_safe_numpy_bool():
    result = make_json_safe(np.bool_(True))
    assert result is True
    assert isinstance(result, bool)


def test_make_json_safe_numpy_integers():
    assert make_json_safe(np.int32(100)) == 100
    assert make_json_safe(np.int64(1000)) == 1000
    assert isinstance(make_json_safe(np.int64(42)), int)


def test_make_json_safe_numpy_floats():
    assert make_json_safe(np.float32(2.5)) == 2.5
    assert make_json_safe(np.float64(3.5)) == 3.5


def test_make_json_safe_numpy_arrays():
    arr = np.array([1, 2, 3])
    assert make_json_safe(arr) == [1, 2, 3]

    arr_with_nan = np.array([1.0, np.nan, 3.0])
    assert make_json_safe(arr_with_nan) == [1.0, None, 3.0]


# Pandas types
def test_make_json_safe_pandas_types():
    assert make_json_safe(pd.NaT) is None
    assert make_json_safe(pd.NA) is None

    ts = pd.Timestamp("2024-01-15 12:30:45")
    assert "2024-01-15T12:30:45" in make_json_safe(ts)

    td = pd.Timedelta(hours=2)
    assert make_json_safe(td) == 7200.0


# Astropy Time objects
def test_make_json_safe_astropy_time():
    try:
        from astropy.time import Time

        t = Time("2024-01-15T12:30:45")
        result = make_json_safe(t)
        assert isinstance(result, str)
        assert "2024-01-15" in result

        # Test with array of times
        t_array = Time(["2024-01-15", "2024-01-16"])
        result = make_json_safe(t_array)
        assert isinstance(result, list)
        assert len(result) == 2
    except ImportError:
        print("Astropy not installed, skipping astropy tests")


# Containers
def test_make_json_safe_containers():
    assert make_json_safe([1, 2, 3]) == [1, 2, 3]
    assert make_json_safe({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    nested = {"values": [np.int64(1), np.nan], "count": np.int32(5)}
    result = make_json_safe(nested)
    assert result == {"values": [1, None], "count": 5}


# JSON serialization
def test_make_json_safe_json_serializable():
    obj = {
        "int": np.int64(42),
        "float": np.float64(3.14),
        "nan": np.nan,
        "timestamp": pd.Timestamp("2024-01-15"),
    }
    result = make_json_safe(obj)
    json_str = json.dumps(result)  # Should not raise
    assert json_str is not None
