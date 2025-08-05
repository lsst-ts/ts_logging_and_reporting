import os
import requests

import lsst.ts.logging_and_reporting.utils as ut

from fastapi.testclient import TestClient
from lsst.ts.logging_and_reporting.web_app.main import app
from lsst.ts.logging_and_reporting.utils import get_access_token
from unittest.mock import patch, Mock

client = TestClient(app)


SERVICE_ENDPOINT_MOCK_RESPONSES = {
    "/exposurelog/instruments": {
        "butler_instruments_1": [
            "LSSTComCamSim",
            "LATISS",
            "LSSTComCam",
            "LSSTCam",
        ],
    },
    "/exposurelog/exposures": [
        {
            "obs_id": "MC_O_20250730_000001",
            "id": 2025073000001,
            "instrument": "LSSTCam",
            "observation_type": "bias",
            "observation_reason": "bias",
            "day_obs": 20250730,
            "seq_num": 1,
            "group_name": "2025-07-30T20:14:23.653",
            "target_name": "azel_target",
            "science_program": "unknown",
            "tracking_ra": None,
            "tracking_dec": None,
            "sky_angle": None,
            "timespan_begin": "2025-07-30T20:14:23.836969",
            "timespan_end": "2025-07-30T20:14:23.849000",
        },
    ],
    "/exposurelog/messages": [
        {
            "id": "6e915887-0dd0-4335-aa30-fa6a8e61660a",
            "site_id": "summit",
            "obs_id": "MC_O_20250730_000001",
            "instrument": "LSSTCam",
            "day_obs": 20250730,
            "seq_num": 1,
            "message_text": (
              "Filter change, the M2 haxapod (strut) went  out of position and returned"
              " to its  previous values\r\n"
            ),
            "level": 10,
            "tags": [
            "undefined"
            ],
            "urls": [],
            "user_id": "test@localhost",
            "user_agent": "exposurelog-service",
            "is_human": True,
            "is_valid": True,
            "exposure_flag": "junk",
            "date_added": "2025-07-30T22:14:23.266086",
            "date_invalidated": None,
            "parent_id": None,
        },
    ],
}

def mock_get_response_generator():
    response_get = requests.Response()
    response_get.status_code = 200
    def response_json_payload():
        called_url = requests.get.call_args[0][0]
        endpoint = called_url.replace(ut.Server.get_url(), "").split("?")[0]
        return SERVICE_ENDPOINT_MOCK_RESPONSES[endpoint]
    response_get.json = response_json_payload
    while True:
        yield response_get

def test_endpoint_auth_header(endpoint):
    headers = {"Authorization": "Bearer header-token"}
    response = client.get(endpoint, headers=headers)
    assert response.status_code == 200

def test_endpoint_auth_env_var(endpoint):
    os.environ["ACCESS_TOKEN"] = "env-token"
    response = client.get(endpoint)
    assert response.status_code == 200
    del os.environ["ACCESS_TOKEN"]

def test_endpoint_auth_rsp_utils(endpoint):
    mock_lsst = Mock()
    mock_lsst.rsp.utils.get_info.return_value = "mocked-token"

    with patch.dict("sys.modules", {
        "lsst": mock_lsst,
        "lsst.rsp.utils": mock_lsst.rsp.utils,
    }):
        response = client.get(endpoint)
        assert response.status_code == 200

def test_endpoint_no_auth(endpoint):
    response = client.get(endpoint)
    assert response.status_code == 401

def test_endpoint_authentication(endpoint):
    test_endpoint_auth_header(endpoint)
    test_endpoint_auth_env_var(endpoint)
    test_endpoint_auth_rsp_utils(endpoint)
    test_endpoint_no_auth(endpoint)

def test_exposure_entries_endpoint():
    mock_requests_get_patcher = patch("requests.get")
    mock_requests_get = mock_requests_get_patcher.start()
    mock_requests_get.side_effect = mock_get_response_generator()

    endpoint = "/exposure-entries?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
    test_endpoint_authentication(endpoint)

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"
    response = client.get(endpoint)
    assert response.status_code == 200
    data = response.json()
    assert "exposure_entries" in data
    expected_entry_params = [
        "obs_id",
        "id",
        "instrument",
        "observation_type",
        "observation_reason",
        "day_obs",
        "seq_num",
        "group_name",
        "target_name",
        "science_program",
        "tracking_ra",
        "tracking_dec",
        "sky_angle",
        "timespan_begin",
        "timespan_end",
        "exposure_flag",
        "exposure_time",
        "message_text",
    ]
    for entry in data["exposure_entries"]:
        for param in expected_entry_params:
            assert param in entry, f"Missing {param} in exposure entry: {entry}"
    app.dependency_overrides.pop(get_access_token, None)

    mock_requests_get_patcher.stop()


def test_exposures_endpoint(monkeypatch):
    def mock_get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=None):
        return [
            {"exp_time": 10, "can_see_sky": True},
            {"exp_time": 20, "can_see_sky": False},
        ]
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_exposures",
        mock_get_exposures,
    )

    app.dependency_overrides[get_access_token] = lambda: "dummy-token"
    response = client.get(
        "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam&auth_token=dummy-token"
    )
    assert response.status_code == 200
    data = response.json()
    assert "exposures" in data
    assert data["exposures_count"] == 2
    assert data["sum_exposure_time"] == 30
    assert data["on_sky_exposures_count"] == 1
    assert data["total_on_sky_exposure_time"] == 30
    app.dependency_overrides.pop(get_access_token, None)



def test_exposures_auth_header(monkeypatch):
    def mock_get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=None):
        assert auth_token == "header-token"
        return [
            {"exp_time": 10, "can_see_sky": True},
            {"exp_time": 20, "can_see_sky": False},
        ]
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_exposures",
        mock_get_exposures,
    )

    headers = {"Authorization": "Bearer header-token"}
    response = client.get(
        "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam",
        headers=headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["exposures_count"] == 2


def test_exposures_env_var(monkeypatch):
    def mock_get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=None):
        assert auth_token == "env-token"
        return [
            {"exp_time": 10, "can_see_sky": True},
            {"exp_time": 20, "can_see_sky": False},
        ]
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_exposures",
        mock_get_exposures,
    )

    os.environ["ACCESS_TOKEN"] = "env-token"
    response = client.get(
        "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["exposures_count"] == 2
    del os.environ["ACCESS_TOKEN"]


def test_exposures_rsp_utils(monkeypatch):
    def mock_get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=None):
        assert auth_token == "rsp-token"
        return [
            {"exp_time": 10, "can_see_sky": True},
            {"exp_time": 20, "can_see_sky": False},
        ]
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_exposures",
        mock_get_exposures,
    )

    import sys
    import types
    orig_lsst = sys.modules.get("lsst")
    orig_rsp = sys.modules.get("lsst.rsp")
    orig_utils = sys.modules.get("lsst.rsp.utils")

    lsst_mod = types.ModuleType("lsst")
    rsp_mod = types.ModuleType("lsst.rsp")
    utils_mod = types.ModuleType("lsst.rsp.utils")
    utils_mod.get_info = lambda: "rsp-token"

    sys.modules["lsst"] = lsst_mod
    sys.modules["lsst.rsp"] = rsp_mod
    sys.modules["lsst.rsp.utils"] = utils_mod
    lsst_mod.rsp = rsp_mod
    rsp_mod.utils = utils_mod

    try:
        import lsst.rsp.utils
        assert lsst.rsp.utils.get_info() == "rsp-token"

        response = client.get(
        "/exposures?dayObsStart=20240101&dayObsEnd=20240102&instrument=LSSTCam"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["exposures_count"] == 2
    finally:
        if orig_utils is not None:
            sys.modules["lsst.rsp.utils"] = orig_utils
        else:
            del sys.modules["lsst.rsp.utils"]
        if orig_rsp is not None:
            sys.modules["lsst.rsp"] = orig_rsp
        else:
            del sys.modules["lsst.rsp"]
        if orig_lsst is not None:
            sys.modules["lsst"] = orig_lsst
        else:
            del sys.modules["lsst"]


def test_almanac_endpoint(monkeypatch):
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.main.get_almanac",
        lambda dayObsStart, dayObsEnd: {"sunset": 123, "sunrise": 456},
    )
    response = client.get("/almanac?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 200
    data = response.json()
    assert "almanac_info" in data
    assert data["almanac_info"] == {"sunset": 123, "sunrise": 456}
