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

from lsst.ts.logging_and_reporting.services.narrativelog import NarrativeLogService


class StubAdapter:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        return self.payload


def make_message(
    msg_id,
    instrument="LSSTCam",
    date_begin="2025-01-01T22:00:00",
    time_lost=0.0,
    time_lost_type=None,
):
    return {
        "id": msg_id,
        "instrument": instrument,
        "date_begin": date_begin,
        "time_lost": time_lost,
        "time_lost_type": time_lost_type,
    }


def make_service(payload):
    adapter = StubAdapter(payload)
    return NarrativeLogService(narrativelog_adapter=adapter), adapter


class TestNarrativeLogService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service({})
        service.handle_request(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [(20250101, 20250102)]

    def test_filters_by_instrument(self):
        payload = {
            20250101: [
                make_message("keep", instrument="LSSTCam"),
                make_message("drop", instrument="LATISS"),
                make_message("drop-none", instrument=None),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert [m["id"] for m in response["narrative_log"]] == ["keep"]

    def test_messages_sorted_newest_first_across_days(self):
        payload = {
            20250101: [make_message("older", date_begin="2025-01-01T20:00:00")],
            20250102: [make_message("newer", date_begin="2025-01-02T20:00:00")],
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250103, "LSSTCam")
        assert [m["id"] for m in response["narrative_log"]] == ["newer", "older"]

    def test_time_lost_summed_by_type(self):
        payload = {
            20250101: [
                make_message("w1", time_lost=1.5, time_lost_type="weather"),
                make_message("w2", time_lost=1.0, time_lost_type="weather"),
                make_message("f1", time_lost=0.25, time_lost_type="fault"),
                make_message("n1", time_lost=3.0, time_lost_type=None),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert response["time_lost_to_weather"] == 2.5
        assert response["time_lost_to_faults"] == 0.25

    def test_other_instrument_excluded_from_time_lost(self):
        payload = {
            20250101: [
                make_message("w1", time_lost=1.5, time_lost_type="weather"),
                make_message("w2", instrument="LATISS", time_lost=4.0, time_lost_type="weather"),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert response["time_lost_to_weather"] == 1.5

    def test_empty_range_returns_zero_totals(self):
        service, _ = make_service({20250101: []})
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert response == {
            "narrative_log": [],
            "time_lost_to_weather": 0,
            "time_lost_to_faults": 0,
        }
