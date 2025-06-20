
from fastapi import Request, Depends
from fastapi.testclient import TestClient
from fastapi import FastAPI
from unittest.mock import patch, Mock
from lsst.ts.logging_and_reporting.utils import get_access_token

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
