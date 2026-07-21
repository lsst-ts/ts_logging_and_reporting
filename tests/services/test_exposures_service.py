from unittest.mock import patch

import pandas as pd

from lsst.ts.logging_and_reporting.services.exposures import ExposuresService

EXPOSURES = "lsst.ts.logging_and_reporting.services.exposures"


class StubConsdbAdapter:
    """Stands in for the ConsDB adapter; returns canned per-dayobs rows."""

    def __init__(self, per_day):
        self.per_day = per_day
        self.calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.calls.append((instrument, start_dayobs, end_dayobs))
        return self.per_day


def make_service(per_day):
    return ExposuresService(adapters={"consdb": StubConsdbAdapter(per_day)})


def no_dome():
    """get_open_close_dome returns None -> no dome data, no error."""
    return patch(f"{EXPOSURES}.get_open_close_dome", return_value=None)


def no_time_accounting(value=None):
    return patch(f"{EXPOSURES}.get_time_accounting", return_value=value)


class TestCollation:
    def test_projects_curated_columns(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10, "extra_col": "drop me"}]}
        with no_dome(), no_time_accounting():
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")
        exposure = response["exposures"][0]
        assert "extra_col" not in exposure
        assert exposure["exposure_id"] == 1
        assert exposure["band"] is None  # curated column absent from the record

    def test_counts_and_durations(self):
        per_day = {
            20250101: [
                {"exposure_id": 1, "seq_num": 1, "can_see_sky": True, "exp_time": 10},
                {"exposure_id": 2, "seq_num": 2, "can_see_sky": False, "exp_time": 20},
                {"exposure_id": 3, "seq_num": 3, "can_see_sky": True, "exp_time": 5},
            ]
        }
        with no_dome(), no_time_accounting():
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")
        assert response["exposures_count"] == 3
        assert response["sum_exposure_time"] == 35
        assert response["on_sky_exposures_count"] == 2
        assert response["total_on_sky_exposure_time"] == 15

    def test_orders_by_dayobs_then_seq_num(self):
        per_day = {
            20250102: [{"exposure_id": 4, "seq_num": 1}],
            20250101: [{"exposure_id": 2, "seq_num": 2}, {"exposure_id": 1, "seq_num": 1}],
        }
        with no_dome(), no_time_accounting():
            response = make_service(per_day).handle(20250101, 20250103, "LSSTCam", "token")
        assert [exposure["exposure_id"] for exposure in response["exposures"]] == [1, 2, 4]


class TestExclusiveEnd:
    def test_converts_exclusive_end_to_inclusive_before_fetch(self):
        adapter = StubConsdbAdapter({})
        service = ExposuresService(adapters={"consdb": adapter})
        with no_dome(), no_time_accounting():
            service.handle(20250101, 20250103, "LSSTCam", "token")
        assert adapter.calls == [("LSSTCam", 20250101, 20250102)]


class TestGracefulDegradation:
    def test_dome_failure_reports_error_and_keeps_exposures(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        with (
            patch(f"{EXPOSURES}.get_open_close_dome", side_effect=RuntimeError("dome down")),
            no_time_accounting(),
        ):
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")
        assert response["open_dome_error"] == "Failed to retrieve dome open/close times"
        assert response["exposures_count"] == 1

    def test_dome_aggregation_failure_reports_error_but_keeps_times(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        dome = pd.DataFrame(
            [{"day_obs": 20250730, "night_hours": 12.0, "open_hours": 9.0, "sunset12": "s", "sunrise12": "r"}]
        )
        with (
            patch(f"{EXPOSURES}.get_open_close_dome", return_value=dome),
            patch(f"{EXPOSURES}._compute_closed_hours", side_effect=RuntimeError("aggregation failed")),
            no_time_accounting(),
        ):
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")
        assert response["open_dome_error"] == "Failed to aggregate dome open hours"
        assert response["day_obs_open_dome_hours"] is None
        assert len(response["open_dome_times"]) == 1  # retrieval succeeded

    def test_time_accounting_failure_reports_error(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        with no_dome(), patch(f"{EXPOSURES}.get_time_accounting", side_effect=RuntimeError("ta down")):
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")
        assert response["time_accounting_error"] == "Failed to compute night time accounting"
        assert response["night_on_sky_time_accounting"] is None

    def test_time_accounting_value_passed_through(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        with no_dome(), no_time_accounting({"on_sky_hours": 3.5}):
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")
        assert response["night_on_sky_time_accounting"] == {"on_sky_hours": 3.5}
        assert response["time_accounting_error"] is None


class TestDomeAggregation:
    def test_open_dome_times_and_per_night_hours(self):
        per_day = {20250101: [{"exposure_id": 1, "seq_num": 1, "exp_time": 10}]}
        dome = pd.DataFrame(
            [
                {
                    "day_obs": 20250730,
                    "open_time": "2025-07-30T23:11:57",
                    "night_hours": 12.0,
                    "open_hours": 9.06,
                    "sunset12": "2025-07-30T23:00:00Z",
                    "sunrise12": "2025-07-31T11:00:00Z",
                }
            ]
        )
        with (
            patch(f"{EXPOSURES}.get_open_close_dome", return_value=dome),
            patch(f"{EXPOSURES}._compute_closed_hours", return_value=7.0),
            no_time_accounting(),
        ):
            response = make_service(per_day).handle(20250101, 20250102, "LSSTCam", "token")

        assert response["open_dome_times"] == [
            {
                "day_obs": 20250730,
                "open_time": "2025-07-30T23:11:57",
                "night_hours": 12.0,
                "open_hours": 9.06,
                "sunset12": "2025-07-30T23:00:00Z",
                "sunrise12": "2025-07-31T11:00:00Z",
            }
        ]
        night = response["day_obs_open_dome_hours"][20250730]
        assert night["night_hours"] == 12.0
        assert night["open_hours"] == 9.06
        assert night["closed_hours"] == 7.0
        assert response["open_dome_error"] is None
