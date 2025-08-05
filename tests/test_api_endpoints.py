from fastapi.testclient import TestClient
from lsst.ts.logging_and_reporting.web_app.main import app

client = TestClient(app)

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
    from lsst.ts.logging_and_reporting.utils import get_access_token
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
    assert data["total_on_sky_exposure_time"] == 10
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
    import os
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
