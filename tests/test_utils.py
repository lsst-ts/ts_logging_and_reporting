import json
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from lsst.ts.logging_and_reporting.utils import get_access_token, make_json_safe, stringify_special_floats

app = FastAPI()


@app.get("/test-access-token")
def access_token_endpoint(
    request: Request = None,
    auth_token: str = Depends(get_access_token),
):
    return {"token": auth_token}


def test_get_access_token_env_variable(monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "env_token")
    token = get_access_token()
    assert token == "env_token"


def test_get_access_token_rsp_utils():
    mock_lsst = Mock()
    mock_lsst.rsp.utils.get_info.return_value = "mocked-token"

    with patch.dict(
        "sys.modules",
        {
            "lsst": mock_lsst,
            "lsst.rsp.utils": mock_lsst.rsp.utils,
        },
    ):
        token = get_access_token()
        assert token == "mocked-token"


def test_get_access_token_request_headers(monkeypatch):
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-access-token", headers={"Authorization": "Bearer header_token"})
    assert response.status_code == 200
    assert response.json() == {"token": "header_token"}


def test_get_access_token_no_token(monkeypatch):
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-access-token")
    assert response.status_code == 401
    assert response.json() == {"detail": "RSP authentication token could not be retrieved by any method."}


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
