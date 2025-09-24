import math
import numpy as np

from fastapi import Request, Depends
from fastapi.testclient import TestClient
from fastapi import FastAPI
from unittest.mock import patch, Mock
from lsst.ts.logging_and_reporting.utils import get_access_token, stringify_special_floats, make_json_safe

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

    with patch.dict("sys.modules", {
        "lsst": mock_lsst,
        "lsst.rsp.utils": mock_lsst.rsp.utils,
    }):
        token = get_access_token()
        assert token == "mocked-token"


def test_get_access_token_request_headers(monkeypatch):
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get(
        "/test-access-token",
        headers={"Authorization": "Bearer header_token"}
    )
    assert response.status_code == 200
    assert response.json() == {"token": "header_token"}


def test_get_access_token_no_token(monkeypatch):
    monkeypatch.delenv("ACCESS_TOKEN", raising=False)
    client = TestClient(app)
    response = client.get("/test-access-token")
    assert response.status_code == 401
    assert response.json() == {
        "detail": "RSP authentication token could not be retrieved by any method."
    }


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


def test_make_json_safe_nan_and_inf():
    assert make_json_safe(np.nan) is None
    assert make_json_safe(np.inf) is None
    assert make_json_safe(-np.inf) is None


def test_make_json_safe_numpy_integers_and_floats():
    assert make_json_safe(np.int32(5)) == 5
    assert make_json_safe(np.int64(10)) == 10
    assert math.isclose(make_json_safe(np.float32(3.14)), 3.14, rel_tol=1e-6)
    assert math.isclose(make_json_safe(np.float64(2.71)), 2.71, rel_tol=1e-12)


def test_make_json_safe_dict_and_list_recursion():
    obj = {
        "a": np.nan,
        "b": [np.int32(1), np.float64(2.5), np.inf],
        "c": {"nested": -np.inf},
    }
    result = make_json_safe(obj)
    assert result == {
        "a": None,
        "b": [1, 2.5, None],
        "c": {"nested": None},
    }


def test_make_json_safe_regular_python_types():
    assert make_json_safe(123) == 123
    assert make_json_safe("abc") == "abc"
    assert make_json_safe([1, 2, 3]) == [1, 2, 3]
    assert make_json_safe({"x": "y"}) == {"x": "y"}
