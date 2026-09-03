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

from lsst.ts.logging_and_reporting.services.nightreport import NightReportService


class StubAdapter:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        return self.payload


def make_report(day_obs, report_id="report-1"):
    return {"id": report_id, "day_obs": day_obs, "summary": "summary"}


def make_service(payload):
    adapter = StubAdapter(payload)
    return NightReportService(nightreport_adapter=adapter), adapter


class TestNightReportService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service({})
        service.handle_request(20250101, 20250103)
        assert adapter.calls == [(20250101, 20250102)]

    def test_reports_sorted_newest_first_across_days(self):
        payload = {
            20250101: [make_report(20250101, "older")],
            20250102: [make_report(20250102, "newer")],
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250103)
        assert [r["id"] for r in response["reports"]] == ["newer", "older"]

    def test_empty_range_returns_empty_reports(self):
        service, _ = make_service({20250101: []})
        assert service.handle_request(20250101, 20250102) == {"reports": []}
