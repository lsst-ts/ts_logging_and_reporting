#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from unittest.mock import Mock

import pandas as pd
import pytest
from matplotlib import pyplot as plt

from lsst.ts.logging_and_reporting.web_app.services import (
    almanac_service,
    consdb_service,
    jira_service,
    rubin_nights_service,
    scheduler_service,
    zephyr_service,
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


def test_compute_nvisits_bundle_uses_static_map_plot_config(monkeypatch):
    captured = {}

    class DummyMetricBundle:
        def __init__(self, metric, slicer, constraint, plot_funcs=None, plot_dict=None):
            captured["plot_dict"] = plot_dict

    class DummyMetricBundleGroup:
        def __init__(self, bundles, db_obj, save_early=False):
            self.bundles = bundles

        def run_current(self, constraint, map_data):
            captured["run_current_args"] = (constraint, map_data)

    monkeypatch.setattr(scheduler_service.maf, "MetricBundle", DummyMetricBundle)
    monkeypatch.setattr(scheduler_service.maf, "MetricBundleGroup", DummyMetricBundleGroup)

    map_data = [{"s_ra": 10.0, "s_dec": -20.0, "sky_rotation": 45.0, "obs_start_mjd": 60000.0}]
    scheduler_service._compute_nvisits_bundle(map_data)

    assert captured["plot_dict"]["title"] == ""
    assert captured["plot_dict"]["bgcolor"] == scheduler_service.COLOR_BG
    assert captured["plot_dict"]["badcolor"] == scheduler_service.COLOR_BG
    assert captured["run_current_args"] == ("", map_data)


def test_build_static_visit_map_styles_and_adds_graticules(monkeypatch):
    fig, ax = plt.subplots()
    ax.imshow([[1, 2], [3, 4]])

    style_calls = []
    graticule_calls = []

    class DummyBundle:
        def plot(self):
            return {"SkyMap": fig.number}

    def fake_style_figure(styled_fig, main_ax):
        style_calls.append((styled_fig, main_ax))

    def fake_add_graticules(main_ax):
        graticule_calls.append(main_ax)

    monkeypatch.setattr(scheduler_service, "_compute_nvisits_bundle", lambda map_data: DummyBundle())
    monkeypatch.setattr(scheduler_service, "_style_figure", fake_style_figure)
    monkeypatch.setattr(scheduler_service, "_add_graticules", fake_add_graticules)

    try:
        png_bytes = scheduler_service.build_static_visit_map(
            [{"s_ra": 10.0, "s_dec": -20.0, "sky_rotation": 45.0, "obs_start_mjd": 60000.0}]
        )
    finally:
        plt.close(fig)

    assert png_bytes
    assert style_calls == [(fig, ax)]
    assert graticule_calls == [ax]


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

    def test_get_jira_tickets_passes_correct_args_to_adapter(self, monkeypatch):
        """Ensure JiraAdapter is instantiated and called with correct
        arguments.
        """

        # Mock the adapter class
        mock_adapter_cls = Mock()

        # Mock the instance returned by the adapter
        mock_adapter_instance = Mock()
        mock_adapter_instance.get_obs_issues.return_value = []

        # When JiraAdapter(...) is called, return our mock instance
        mock_adapter_cls.return_value = mock_adapter_instance

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            mock_adapter_cls,
        )

        # Call the function
        jira_service.get_jira_tickets(
            20240101,
            20240102,
            "LATISS",
            jira_token="abc",
            jira_hostname="jira.example.com",
        )

        # Assert constructor was called correctly
        mock_adapter_cls.assert_called_once_with(
            jira_token="abc",
            jira_hostname="jira.example.com",
        )

        # Assert method was called correctly
        mock_adapter_instance.get_obs_issues.assert_called_once_with(
            min_dayobs=20240101,
            max_dayobs=20240102,
        )

    def test_get_jira_tickets_returns_empty_list_when_no_tickets(self, monkeypatch):
        """Test that an empty list is returned when no tickets are found."""

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                self.jira_token = jira_token
                self.jira_hostname = jira_hostname

            def get_obs_issues(self, min_dayobs, max_dayobs):
                return []

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        result = jira_service.get_jira_tickets(20240101, 20240102, "LATISS")
        assert result == []

    def test_get_jira_tickets_returns_empty_list_when_fetch_returns_none(self, monkeypatch):
        """Test that an empty list is returned when get_obs_issues
        returns None.
        """

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                pass

            def get_obs_issues(self, min_dayobs, max_dayobs):
                return None

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        result = jira_service.get_jira_tickets(20240101, 20240102, "LATISS")
        assert result == []

    def test_get_jira_tickets_filters_by_instrument_included(self, monkeypatch, dummy_tickets):
        """Test that tickets are filtered to include only specified
        instruments.
        """

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                pass

            def get_obs_issues(self, min_dayobs, max_dayobs):
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
        """Test that tickets are filtered to exclude specified instruments
        (defined in INSTRUMENT_EXCLUDE_MAP).
        """

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                pass

            def get_obs_issues(self, min_dayobs, max_dayobs):
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


@pytest.mark.asyncio
async def test_get_test_cases_returns_names():
    """Valid keys should return mapping of key -> test case name."""

    class DummyZephyr:
        async def get_test_case(self, key):
            return {"name": f"Name for {key}"}

    keys = ["BLOCK-T123", "BLOCK-T456"]

    result = await zephyr_service.get_test_cases(
        keys,
        zephyr=DummyZephyr(),
    )

    assert result == {
        "BLOCK-T123": "Name for BLOCK-T123",
        "BLOCK-T456": "Name for BLOCK-T456",
    }


@pytest.mark.asyncio
async def test_get_test_cases_uses_parent_key_for_suffix():
    """Keys with suffix should query parent but return original key."""

    called_with = []

    class DummyZephyr:
        async def get_test_case(self, key):
            called_with.append(key)
            return {"name": "Parent name"}

    keys = ["BLOCK-T123_a"]

    result = await zephyr_service.get_test_cases(
        keys,
        zephyr=DummyZephyr(),
    )

    assert called_with == ["BLOCK-T123"]
    assert result == {"BLOCK-T123_a": "Parent name"}


@pytest.mark.asyncio
async def test_get_test_cases_skips_failed_retrieval():
    """If Zephyr raises for a key, it should be skipped."""

    class DummyZephyr:
        async def get_test_case(self, key):
            if key == "BLOCK-T123":
                raise Exception("fail")
            return {"name": "OK"}

    keys = ["BLOCK-T123", "BLOCK-T456"]

    result = await zephyr_service.get_test_cases(
        keys,
        zephyr=DummyZephyr(),
    )

    assert result == {"BLOCK-T456": "OK"}


@pytest.mark.asyncio
async def test_get_test_cases_empty_input():
    """Empty key list should return empty dict."""

    result = await zephyr_service.get_test_cases(
        [],
        zephyr=object(),  # not used
    )

    assert result == {}


class TestGetBlockTicketSummaries:
    """Tests for the get_block_ticket_summaries function."""

    def test_returns_empty_dict_when_no_ticket_keys(self, monkeypatch):
        """Test that an empty dict is returned when no ticket keys are
        provided.
        """

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                pass

            def fetch_block_ticket_summaries(self, ticket_keys):
                return {"SHOULD": "NOT BE CALLED"}

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        result = jira_service.get_block_ticket_summaries([])
        assert result == {}

    def test_returns_empty_dict_when_fetch_returns_none(self, monkeypatch):
        """Test that an empty dict is returned when
        fetch_block_ticket_summaries returns an empty dict.
        """

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                pass

            def fetch_block_ticket_summaries(self, ticket_keys):
                return {}

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        result = jira_service.get_block_ticket_summaries(["BLOCK-1"])
        assert result == {}

    def test_returns_ticket_summaries_correctly(self, monkeypatch):
        """Test that the function returns the expected ticket summaries."""

        class DummyJiraAdapter:
            def __init__(self, jira_token=None, jira_hostname=None):
                pass

            def fetch_block_ticket_summaries(self, ticket_keys):
                return {key: f"Summary for {key}" for key in ticket_keys}

        monkeypatch.setattr(
            "lsst.ts.logging_and_reporting.web_app.services.jira_service.JiraAdapter",
            DummyJiraAdapter,
        )

        ticket_keys = ["BLOCK-1", "BLOCK-2"]
        result = jira_service.get_block_ticket_summaries(ticket_keys)

        expected = {
            "BLOCK-1": "Summary for BLOCK-1",
            "BLOCK-2": "Summary for BLOCK-2",
        }

        assert result == expected


def make_event(status=1, time_ms=0, time="t", note="n", labels=None):
    return {
        "status": status,
        "time_ms": time_ms,
        "time": time,
        "note": note,
        "statusLabels": labels or [],
    }


def make_interval_event(start_state=1, start_time_ms=0, end_time_ms=3600000):
    return {
        "start_state": start_state,
        "start_time_ms": start_time_ms,
        "end_time_ms": end_time_ms,
    }


# must be in sync with make_almanac
def make_night(start=0, end=3600000):
    return {"start_ms": start, "end_ms": end}


# must be in sync with make_night
def make_almanac():
    return [
        {
            "twilight_evening": "1970-01-01 00:00:00",
            "twilight_morning": "1970-01-01 01:00:00",
        }
    ]


def patch_events(monkeypatch, events):
    monkeypatch.setattr(
        rubin_nights_service,
        "get_obs_status_events",
        lambda *args, **kwargs: events,
    )


def patch_almanac(monkeypatch, almanac):
    monkeypatch.setattr(
        rubin_nights_service,
        "get_almanac",
        lambda *args, **kwargs: almanac,
    )


class TestObsStatusEvents:
    """Tests for get_obs_status_events."""

    def test_get_obs_status_events_returns_records(self, monkeypatch):
        class DummyEfd:
            def select_time_series(self, topic, fields, t_start, t_end):
                return pd.DataFrame(
                    [
                        {
                            "status": 1,
                            "note": "n",
                            "statusLabels": [],
                        }
                    ],
                    index=[pd.Timestamp("1970-01-01T00:00:00")],
                )

        class DummyClients:
            def __getitem__(self, key):
                return DummyEfd()

        monkeypatch.setattr(
            rubin_nights_service,
            "get_clients",
            lambda auth_token=None: {"efd": DummyClients()["efd"]},
        )

        result = rubin_nights_service.get_obs_status_events(
            19700101,
            19700101,
            auth_token=None,
        )

        assert isinstance(result, list)
        assert len(result) == 1
        assert "time_ms" in result[0]

    def test_get_obs_status_events_returns_empty_list_on_error(self, monkeypatch):
        monkeypatch.setattr(
            rubin_nights_service,
            "get_clients",
            lambda auth_token=None: (_ for _ in ()).throw(Exception("fail")),
        )

        result = rubin_nights_service.get_obs_status_events(
            19700101,
            19700101,
        )

        assert result == []


class TestIntervals:
    """Tests for interval construction and overlap logic."""

    def test_get_obs_status_intervals_single(self):
        events = [
            make_event(status=1, time_ms=0),
            make_event(status=2, time_ms=1000),
        ]

        result = rubin_nights_service.get_obs_status_intervals(events)

        assert len(result) == 1
        assert result[0]["interval_length_ms"] == 1000
        assert result[0]["start_state"] == 1
        assert result[0]["end_state"] == 2
        assert result[0]["changed_mask"] == 3

    def test_get_obs_status_intervals_no_events(self):

        assert rubin_nights_service.get_obs_status_intervals([]) == []

    def test_decode_states_single_bitmask(self):
        mask = rubin_nights_service.OBSERVATORY_STATES["FAULT"]

        decoded = rubin_nights_service.decode_states(mask)

        assert "FAULT" in decoded
        assert isinstance(decoded, list)

    def test_decode_states_combined_bitmask(self):
        mask = (
            rubin_nights_service.OBSERVATORY_STATES["FAULT"]
            | rubin_nights_service.OBSERVATORY_STATES["WEATHER"]
        )

        decoded = rubin_nights_service.decode_states(mask)

        assert "FAULT" in decoded
        assert "WEATHER" in decoded

    def test_decode_states_unknown(self):
        mask = rubin_nights_service.OBSERVATORY_STATES["UNKNOWN"]

        decoded = rubin_nights_service.decode_states(mask)

        assert "UNKNOWN" in decoded
        assert isinstance(decoded, list)

    # TODO: WEATHER (8) == WEATHER | UNKNOWN (8) ?
    def test_decode_states_weather_implies_unknown(self):
        mask = rubin_nights_service.OBSERVATORY_STATES["WEATHER"]

        decoded = rubin_nights_service.decode_states(mask)

        assert "WEATHER" in decoded
        assert "UNKNOWN" in decoded


class TestAlmanacHandling:
    """Tests for almanac conversion to ms intervals."""

    def test_build_ms_night_intervals(self):
        result = rubin_nights_service.build_ms_night_intervals(make_almanac())

        assert result == [make_night()]

    def test_almanac_to_unix_ms(self):
        ts = "1970-01-01 00:00:00"

        result = rubin_nights_service.almanac_to_unix_ms(ts)

        assert result == 0


class TestPredicates:
    """Tests for status predicate functions."""

    def test_is_unknown(self):
        status = rubin_nights_service.OBSERVATORY_STATES["UNKNOWN"]
        assert rubin_nights_service.is_unknown(status)

    def test_contains_fault_true(self):
        status = rubin_nights_service.OBSERVATORY_STATES["FAULT"]
        assert rubin_nights_service.contains_fault(status)

    def test_contains_fault_false(self):
        status = rubin_nights_service.OBSERVATORY_STATES["DAYTIME"]
        assert not rubin_nights_service.contains_fault(status)

    def test_counts_as_fault_loss_true(self):
        status = (
            rubin_nights_service.OBSERVATORY_STATES["FAULT"]
            | rubin_nights_service.OBSERVATORY_STATES["WEATHER"]
        )
        assert rubin_nights_service.counts_as_fault_loss(status)

    def test_counts_as_fault_loss_false(self):
        status = (
            rubin_nights_service.OBSERVATORY_STATES["FAULT"]
            | rubin_nights_service.OBSERVATORY_STATES["DOWNTIME"]
        )
        assert not rubin_nights_service.counts_as_fault_loss(status)

    # TODO: WEATHER (8) == WEATHER | UNKNOWN (8) ?
    def test_counts_as_unknown(self):
        status = rubin_nights_service.OBSERVATORY_STATES["WEATHER"]
        assert rubin_nights_service.counts_as_unknown(status)


class TestSumIntervalOverlap:
    """Tests for interval overlap computation."""

    def test_sum_interval_overlap_full(self):
        event_intervals = [make_interval_event()]

        night_intervals = [make_night()]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: True,
        )

        assert result == 1.0

    def test_sum_interval_overlap_partial_start(self):
        event_intervals = [make_interval_event()]

        night_intervals = [make_night(start=1800000, end=5400000)]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: True,
        )

        assert result == 0.5

    def test_sum_interval_overlap_partial_end(self):
        event_intervals = [make_interval_event(start_state=1, start_time_ms=1800000, end_time_ms=5000000)]

        night_intervals = [make_night()]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: True,
        )

        assert result == 0.5

    def test_sum_interval_no_overlap(self):
        event_intervals = [make_interval_event(start_state=1, start_time_ms=3700000, end_time_ms=5000000)]

        night_intervals = [make_night()]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: True,
        )

        assert result == 0.0

    def test_sum_interval_overlap_filtered_out(self):
        event_intervals = [make_interval_event()]

        night_intervals = [make_night()]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: False,
        )

        assert result == 0.0

    def test_sum_interval_overlap_multiple_nights(self):
        event_intervals = [
            # 0.5 hrs overlap on first night
            make_interval_event(start_state=1, start_time_ms=1800000, end_time_ms=4000000),
            # interval between nights
            make_interval_event(start_state=1, start_time_ms=4000000, end_time_ms=4500000),
            # 0.5 hrs overlap on second night
            make_interval_event(start_state=1, start_time_ms=6000000, end_time_ms=7800000),
        ]

        night_intervals = [
            make_night(start=0, end=3600000),
            make_night(start=5000000, end=8600000),
        ]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: True,
        )

        assert result == 1.0

    def test_sum_interval_overlap_multinight_interval(self):
        # Testing extreme edge case

        event_intervals = [
            # 1.0 hr overlap on first night +
            # 0.5 hrs overlap on second night.
            make_interval_event(start_state=1, start_time_ms=0, end_time_ms=6800000),
        ]

        night_intervals = [
            make_night(start=0, end=3600000),
            make_night(start=5000000, end=8600000),
        ]

        result = rubin_nights_service.sum_interval_overlap(
            event_intervals,
            night_intervals,
            lambda _: True,
        )

        assert result == 1.5


class TestGetObsStatus:
    """Integration tests for get_obs_status."""

    def test_get_obs_status_defaults(self, monkeypatch):
        patch_events(
            monkeypatch,
            [make_event()],
        )

        result = rubin_nights_service.get_obs_status(
            19700101,
            19700102,
        )

        assert "entries" in result
        assert "intervals" not in result
        assert "metrics" not in result

    def test_get_obs_status_entries_only(self, monkeypatch):
        patch_events(
            monkeypatch,
            [make_event()],
        )

        result = rubin_nights_service.get_obs_status(
            19700101,
            19700102,
            include_entries=True,
            include_intervals=False,
            requested_metrics=None,
        )

        assert "entries" in result
        assert "intervals" not in result
        assert "metrics" not in result

    def test_get_obs_status_intervals_only(self, monkeypatch):
        patch_events(
            monkeypatch,
            [make_event(), make_event(time_ms=1000)],
        )

        result = rubin_nights_service.get_obs_status(
            19700101,
            19700102,
            include_entries=False,
            include_intervals=True,
            requested_metrics=None,
        )

        assert "entries" not in result
        assert "metrics" not in result
        assert "intervals" in result
        assert len(result["intervals"]) == 1

    def test_get_obs_status_metrics_only(self, monkeypatch):
        patch_events(
            monkeypatch,
            [
                make_event(status=rubin_nights_service.OBSERVATORY_STATES["FAULT"]),
                make_event(
                    status=rubin_nights_service.OBSERVATORY_STATES["FAULT"],
                    time_ms=3600000,
                ),
            ],
        )

        patch_almanac(monkeypatch, make_almanac())

        result = rubin_nights_service.get_obs_status(
            19700101,
            19700102,
            include_entries=False,
            include_intervals=False,
            requested_metrics=["fault_loss"],
        )

        assert "entries" not in result
        assert "intervals" not in result
        assert "metrics" in result
        assert "fault_loss" in result["metrics"]
        assert result["metrics"]["fault_loss"] == 1.0

    def test_get_obs_status_multiple_metrics(self, monkeypatch):
        patch_events(
            monkeypatch,
            [
                make_event(status=rubin_nights_service.OBSERVATORY_STATES["FAULT"]),
                make_event(
                    status=rubin_nights_service.OBSERVATORY_STATES["FAULT"],
                    time_ms=3600000,
                ),
            ],
        )

        patch_almanac(monkeypatch, make_almanac())

        result = rubin_nights_service.get_obs_status(
            19700101,
            19700102,
            requested_metrics=["fault_loss", "operational"],
        )

        assert "intervals" not in result
        assert "metrics" in result
        assert "fault_loss" in result["metrics"]
        assert "operational" in result["metrics"]

    def test_get_obs_status_empty_metric(self, monkeypatch):
        patch_events(monkeypatch, [])
        patch_almanac(monkeypatch, make_almanac())

        result = rubin_nights_service.get_obs_status(
            20250101,
            20250102,
            requested_metrics=[],
        )

        assert "metrics" not in result

    def test_get_obs_status_invalid_metric(self, monkeypatch, caplog):
        patch_events(monkeypatch, [])
        patch_almanac(monkeypatch, make_almanac())

        result = rubin_nights_service.get_obs_status(
            20250101,
            20250102,
            requested_metrics=["invalid_metric"],
        )

        warnings = [record.message for record in caplog.records if record.levelname == "WARNING"]

        assert "Unknown metric requested: invalid_metric" in warnings
        assert result["metrics"] == {}

    def test_get_obs_status_failure_returns_empty_dict(self, monkeypatch):

        def raise_error(*args, **kwargs):
            raise Exception("fail")

        monkeypatch.setattr(
            rubin_nights_service,
            "get_obs_status_events",
            raise_error,
        )

        result = rubin_nights_service.get_obs_status(
            19700101,
            19700102,
        )

        assert result == {}
