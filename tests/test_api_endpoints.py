from fastapi.testclient import TestClient
from lsst.ts.logging_and_reporting.web_app.main import app

client = TestClient(app)

def test_exposures_endpoint(monkeypatch):
    # Mock get_exposures to avoid real DB/API calls
    def mock_get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=None):
        return [
            {"exp_time": 10, "can_see_sky": True},
            {"exp_time": 20, "can_see_sky": False},
        ]
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.services.consdb_service.get_exposures",
        mock_get_exposures,
    )
    # Also mock get_access_token
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.utils.get_access_token",
        lambda: "dummy-token",
    )
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

def test_almanac_endpoint(monkeypatch):
    # Mock get_almanac to avoid real DB/API calls
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.services.almanac_service.get_almanac",
        lambda dayObsStart, dayObsEnd: {"sunset": 123, "sunrise": 456},
    )
    response = client.get("/almanac?dayObsStart=20240101&dayObsEnd=20240102")
    assert response.status_code == 200
    data = response.json()
    assert "almanac_info" in data
    assert data["almanac_info"] == {"sunset": 123, "sunrise": 456}
