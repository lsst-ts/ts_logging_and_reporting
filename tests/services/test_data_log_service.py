from lsst.ts.logging_and_reporting.services.data_log import DataLogService


class StubConsdbAdapter:
    """Stands in for the ConsDB adapter; returns canned per-dayobs rows."""

    def __init__(self, per_day):
        self.per_day = per_day
        self.calls = []

    def fetch(self, instrument, start_dayobs, end_dayobs):
        self.calls.append((instrument, start_dayobs, end_dayobs))
        return self.per_day


def make_service(per_day):
    return DataLogService(adapters={"consdb": StubConsdbAdapter(per_day)})


class TestDataLog:
    def test_flattens_records_in_dayobs_order(self):
        per_day = {
            20250102: [{"exposure_id": 2, "day_obs": 20250102}],
            20250101: [{"exposure_id": 1, "day_obs": 20250101}],
        }
        response = make_service(per_day).handle(20250101, 20250103, "LSSTCam")
        assert [record["exposure_id"] for record in response["data_log"]] == [1, 2]

    def test_special_floats_rendered_as_strings(self):
        per_day = {20250101: [{"exposure_id": 1, "value": float("nan")}]}
        response = make_service(per_day).handle(20250101, 20250102, "LSSTCam")
        assert response["data_log"][0]["value"] == "NaN"

    def test_missing_numeric_becomes_string_nan(self):
        # The DataFrame gives the second row a NaN for the absent column.
        per_day = {20250101: [{"exposure_id": 1, "value": 5}, {"exposure_id": 2}]}
        response = make_service(per_day).handle(20250101, 20250102, "LSSTCam")
        assert response["data_log"][1]["value"] == "NaN"

    def test_empty_range_returns_empty_list(self):
        response = make_service({20250101: []}).handle(20250101, 20250102, "LSSTCam")
        assert response["data_log"] == []

    def test_converts_exclusive_end_to_inclusive_before_fetch(self):
        adapter = StubConsdbAdapter({})
        DataLogService(adapters={"consdb": adapter}).handle(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [("LSSTCam", 20250101, 20250102)]
