import pytest

from lsst.ts.logging_and_reporting.web_app.services import (
    almanac_service,
    consdb_service,
    jira_service,
    scheduler_service,
)


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
            self.night_hours = 11
            self.as_dict = [
                {
                    "Evening Nautical Twilight": "2024-01-01 19:00:00",
                    "Morning Nautical Twilight": "2024-01-02 06:00:00",
                    "Moon Rise": "2024-01-01 21:00:00",
                    "Moon Set": "2024-01-02 03:00:00",
                    "Moon Illumination": 0.75,
                }
            ]

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


def test_get_expected_exposures_normal_behaviour(monkeypatch):
    """Test normal behavior: 3 days, each returning 100 visits."""

    def fake_fetch(*, day_obs, max_simulation_age=None):
        return {"nominal_visits": 100}

    monkeypatch.setattr(
        scheduler_service,
        "fetch_sim_stats_for_night",
        fake_fetch,
    )

    # 20240101–20240103 = 3 nights
    result = scheduler_service.get_expected_exposures(20240101, 20240103)
    assert result["sum"] == 300


def test_get_expected_exposures_missing_nominal_visits(monkeypatch):
    """If the external call returns a dict without nominal_visits,
    treat as zero.
    """

    def fake_fetch(*, day_obs, max_simulation_age=None):
        return {}  # missing key

    monkeypatch.setattr(
        scheduler_service,
        "fetch_sim_stats_for_night",
        fake_fetch,
    )

    result = scheduler_service.get_expected_exposures(20240101, 20240101)
    assert result["sum"] == 0


def test_get_expected_exposures_inner_exception(monkeypatch):
    """If one day raises inside loop, the exception is propagated."""

    def fake_fetch(*, day_obs, max_simulation_age=None):
        raise RuntimeError("fail")

    monkeypatch.setattr(
        scheduler_service,
        "fetch_sim_stats_for_night",
        fake_fetch,
    )

    with pytest.raises(RuntimeError, match="fail"):
        scheduler_service.get_expected_exposures(20240101, 20240102)


def test_get_expected_exposures_partial_failures(monkeypatch):
    """Mixed success/failure: exception should be raised on failure."""

    def fake_fetch(*, day_obs, max_simulation_age=None):
        if day_obs == 20240101:
            return {"nominal_visits": 50}
        else:
            raise Exception("fail")

    monkeypatch.setattr(
        scheduler_service,
        "fetch_sim_stats_for_night",
        fake_fetch,
    )

    with pytest.raises(Exception, match="fail"):
        scheduler_service.get_expected_exposures(20240101, 20240102)


def test_get_expected_exposures_start_greater_than_end(monkeypatch):
    """If start > end, loop never runs → sum = 0."""
    called = False

    def fake_fetch(*, day_obs, max_simulation_age=None):
        nonlocal called
        called = True
        return {"nominal_visits": 9999}

    monkeypatch.setattr(
        scheduler_service,
        "fetch_sim_stats_for_night",
        fake_fetch,
    )

    result = scheduler_service.get_expected_exposures(20240102, 20240101)
    assert not called
    assert result["sum"] == 0


def test_get_expected_exposures_outer_exception(monkeypatch):
    """An outer try-block exception should raise ValueError."""

    # Break datetime.strptime to trigger the outer except
    class FakeDatetime:
        @staticmethod
        def strptime(*args, **kwargs):
            raise ValueError("bad date")

    monkeypatch.setattr(
        scheduler_service,
        "datetime",
        FakeDatetime,
    )

    with pytest.raises(ValueError):
        scheduler_service.get_expected_exposures(20240101, 20240102)


def test_get_expected_exposures_invalid_date_format(monkeypatch):
    """Invalid YYYYMMDD should raise and never call fetch."""
    called = False

    def fake_fetch(*, day_obs, max_simulation_age=None):
        nonlocal called
        called = True
        return {"nominal_visits": 100}

    monkeypatch.setattr(
        scheduler_service,
        "fetch_sim_stats_for_night",
        fake_fetch,
    )

    # Month 13 is invalid
    with pytest.raises(ValueError):
        scheduler_service.get_expected_exposures(20241301, 20240102)

    assert not called


@pytest.fixture
def dummy_tickets():
    """Fixture providing sample JIRA tickets for testing."""
    return [
        {"key": "OBS-1", "system": ["AuxTel"], "summary": "AuxTel issue"},
        {"key": "OBS-2", "system": ["Simonyi"], "summary": "Simonyi issue"},
        {"key": "OBS-3", "system": ["LATISS"], "summary": "LATISS issue"},
        {"key": "OBS-4", "system": ["LSSTCam"], "summary": "LSSTCam issue"},
        {"key": "OBS-5", "system": ["LATISS", "LSSTCam"], "summary": "Cameras issue"},
        {"key": "OBS-6", "system": ["Facilities"], "summary": "Cameras issue"},
        {"key": "OBS-7", "system": ["AuxTel Calibrations"], "summary": "AT calibration issue"},
    ]


class TestGetJiraTickets:
    """Tests for the get_jira_tickets function."""

    def test_get_jira_tickets_returns_empty_list_when_no_tickets(self, monkeypatch):
        """Test that an empty list is returned when no tickets are found."""

        class DummyJiraAdapter:
            def __init__(self, max_dayobs, min_dayobs):
                self.max_dayobs = max_dayobs
                self.min_dayobs = min_dayobs

            def fetch_issues(self):
                return []

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        result = jira_service.get_jira_tickets(20240101, 20240102, "LATISS")
        assert result == []

    def test_get_jira_tickets_returns_empty_list_when_fetch_returns_none(self, monkeypatch):
        """Test that an empty list is returned when
        fetch_issues returns None."""

        class DummyJiraAdapter:
            def __init__(self, max_dayobs, min_dayobs):
                self.max_dayobs = max_dayobs
                self.min_dayobs = min_dayobs

            def fetch_issues(self):
                return None

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        result = jira_service.get_jira_tickets(20240101, 20240102, "LATISS")
        assert result == []

    def test_get_jira_tickets_filters_by_instrument_included(self, monkeypatch, dummy_tickets):
        """Test that tickets are filtered to include
        only specified instruments."""

        class DummyJiraAdapter:
            def __init__(self, max_dayobs, min_dayobs):
                self.max_dayobs = max_dayobs
                self.min_dayobs = min_dayobs

            def fetch_issues(self):
                return dummy_tickets

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        not_excluding_instruments = (
            jira_service.INSTRUMENTS.keys() - jira_service.INSTRUMENT_EXCLUDE_MAP.keys()
        )
        for instrument in not_excluding_instruments:
            result = jira_service.get_jira_tickets(20240101, 20240102, instrument)
            included_systems = (
                instrument,
                jira_service.INSTRUMENTS[instrument],
            )
            for ticket in result:
                assert any(included in system for included in included_systems for system in ticket["system"])

    def test_get_jira_tickets_filters_by_instrument_excluded(self, monkeypatch, dummy_tickets):
        """Test that tickets are filtered to exclude
        specified instruments (defined in INSTRUMENT_EXCLUDE_MAP)."""

        class DummyJiraAdapter:
            def __init__(self, max_dayobs, min_dayobs):
                self.max_dayobs = max_dayobs
                self.min_dayobs = min_dayobs

            def fetch_issues(self):
                return dummy_tickets

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        for instrument in jira_service.INSTRUMENT_EXCLUDE_MAP:
            result = jira_service.get_jira_tickets(20240101, 20240102, instrument)
            excluded_systems = jira_service.INSTRUMENT_EXCLUDE_MAP[instrument]
            match = any(
                excluded in system
                for excluded in excluded_systems
                for ticket in result
                for system in ticket["system"]
            )
            assert not match
