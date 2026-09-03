# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

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
    return DataLogService(consdb_adapter=StubConsdbAdapter(per_day))


class TestDataLog:
    def test_flattens_records_in_dayobs_order(self):
        per_day = {
            20250102: [{"exposure_id": 2, "day_obs": 20250102}],
            20250101: [{"exposure_id": 1, "day_obs": 20250101}],
        }
        response = make_service(per_day).handle_request(20250101, 20250103, "LSSTCam")
        assert [record["exposure_id"] for record in response["data_log"]] == [1, 2]

    def test_orders_by_dayobs_then_seq_num(self):
        # ConsDB returns each night's rows unordered, so the service has
        # to impose the order rather than trust the adapter's list.
        per_day = {
            20250102: [{"exposure_id": 4, "seq_num": 1}],
            20250101: [{"exposure_id": 2, "seq_num": 2}, {"exposure_id": 1, "seq_num": 1}],
        }
        response = make_service(per_day).handle_request(20250101, 20250103, "LSSTCam")
        assert [record["exposure_id"] for record in response["data_log"]] == [1, 2, 4]

    def test_special_floats_rendered_as_strings(self):
        per_day = {20250101: [{"exposure_id": 1, "value": float("nan")}]}
        response = make_service(per_day).handle_request(20250101, 20250102, "LSSTCam")
        assert response["data_log"][0]["value"] == "NaN"

    def test_missing_numeric_becomes_string_nan(self):
        # The DataFrame gives the second row a NaN for the absent column.
        per_day = {20250101: [{"exposure_id": 1, "value": 5}, {"exposure_id": 2}]}
        response = make_service(per_day).handle_request(20250101, 20250102, "LSSTCam")
        assert response["data_log"][1]["value"] == "NaN"

    def test_empty_range_returns_empty_list(self):
        response = make_service({20250101: []}).handle_request(20250101, 20250102, "LSSTCam")
        assert response["data_log"] == []

    def test_converts_exclusive_end_to_inclusive_before_fetch(self):
        adapter = StubConsdbAdapter({})
        DataLogService(consdb_adapter=adapter).handle_request(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [("LSSTCam", 20250101, 20250102)]
