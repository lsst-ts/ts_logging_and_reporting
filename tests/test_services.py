from lsst.ts.logging_and_reporting.web_app.services import consdb_service
from lsst.ts.logging_and_reporting.web_app.services import almanac_service

class DummyExposure:
    def __init__(self, exp_time, can_see_sky):
        self.exp_time = exp_time
        self.can_see_sky = can_see_sky
    def __getitem__(self, key):
        return getattr(self, key)
    def get(self, key, default=None):
        return getattr(self, key, default)

def test_get_exposures(monkeypatch):
    # Patch any external dependencies if needed
    # Here, just test the function signature and a simple mock
    def mock_get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=None):
        return [
            {"exp_time": 10, "can_see_sky": True},
            {"exp_time": 20, "can_see_sky": False},
        ]
    monkeypatch.setattr(consdb_service, "get_exposures", mock_get_exposures)

    result = consdb_service.get_exposures(20240101, 20240102, "LSSTCam", auth_token="token")
    assert isinstance(result, list)
    assert result[0]["exp_time"] == 10
    assert result[1]["can_see_sky"] is False

def test_get_almanac(monkeypatch):
    class DummyAlmanac:
        def __init__(self, min_dayobs, max_dayobs):
            self.as_dict = [{
                'Evening Nautical Twilight': '2024-01-01 19:00:00',
                'Morning Nautical Twilight': '2024-01-02 06:00:00',
                'Moon Rise': '2024-01-01 21:00:00',
                'Moon Set': '2024-01-02 03:00:00',
                'Moon Illumination': 0.75,
            }]

    # Mock the Almanac class to return fixed data
    monkeypatch.setattr(
        "lsst.ts.logging_and_reporting.web_app.services.almanac_service.Almanac",
        DummyAlmanac,
    )
    result = almanac_service.get_almanac(20240101, 20240102)
    assert isinstance(result, list)
    assert result[0]["night_hours"] == 11
    assert result[0]["moon_illumination"] == 0.75
    assert "twilight_evening" in result[0]
    assert "twilight_morning" in result[0]
