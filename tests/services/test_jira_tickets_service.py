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

from lsst.ts.logging_and_reporting.services.jira import (
    JiraTicketsService,
    filter_tickets_by_instrument,
)


class StubAdapter:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.calls.append((start_dayobs, end_dayobs))
        return self.payload


def make_ticket(key="OBS-1", systems=("Simonyi",), created_utc="2025-01-01T18:00:00+00:00"):
    return {
        "key": key,
        "summary": f"summary of {key}",
        "updated": "2025-01-01 20:00:00",
        "created": "2025-01-01 18:00:00",
        "status": "In Progress",
        "system": list(systems),
        "url": f"https://jira.test/browse/{key}",
        "time_lost": 0.5,
        "created_utc": created_utc,
    }


def make_service(payload):
    adapter = StubAdapter(payload)
    return JiraTicketsService(jira_obs_adapter=adapter), adapter


class TestJiraTicketsService:
    def test_exclusive_end_converted_to_inclusive_fetch(self):
        service, adapter = make_service({})
        service.handle_request(20250101, 20250103, "LSSTCam")
        assert adapter.calls == [(20250101, 20250102)]

    def test_ticket_in_multiple_buckets_returned_once(self):
        ticket = make_ticket()
        payload = {20250101: [ticket], 20250102: [dict(ticket)]}
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250103, "LSSTCam")
        assert [t["key"] for t in response["issues"]] == ["OBS-1"]

    def test_is_new_when_created_within_requested_window(self):
        payload = {
            20250101: [
                make_ticket(key="OBS-NEW", created_utc="2025-01-01T18:00:00+00:00"),
                make_ticket(key="OBS-OLD", created_utc="2024-12-25T18:00:00+00:00"),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        flags = {t["key"]: t["isNew"] for t in response["issues"]}
        assert flags == {"OBS-NEW": True, "OBS-OLD": False}

    def test_created_utc_not_in_response(self):
        service, _ = make_service({20250101: [make_ticket()]})
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert "created_utc" not in response["issues"][0]

    def test_latiss_includes_only_matching_systems(self):
        payload = {
            20250101: [
                make_ticket(key="OBS-AUX", systems=("AuxTel",)),
                make_ticket(key="OBS-LAT", systems=("LATISS calibration",)),
                make_ticket(key="OBS-SIM", systems=("Simonyi",)),
                make_ticket(key="OBS-NONE", systems=()),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LATISS")
        assert {t["key"] for t in response["issues"]} == {"OBS-AUX", "OBS-LAT"}

    def test_lsstcam_excludes_latiss_but_keeps_everything_else(self):
        payload = {
            20250101: [
                make_ticket(key="OBS-AUX", systems=("AuxTel",)),
                make_ticket(key="OBS-SIM", systems=("Simonyi",)),
                make_ticket(key="OBS-NONE", systems=()),
            ]
        }
        service, _ = make_service(payload)
        response = service.handle_request(20250101, 20250102, "LSSTCam")
        assert {t["key"] for t in response["issues"]} == {"OBS-SIM", "OBS-NONE"}


class TestFilterTicketsByInstrument:
    def test_include_keeps_only_matching_systems(self):
        tickets = [
            make_ticket(key="OBS-SIM", systems=("Simonyi",)),
            make_ticket(key="OBS-AUX", systems=("AuxTel",)),
        ]
        result = filter_tickets_by_instrument(tickets, instrument="LSSTCam")
        assert [t["key"] for t in result] == ["OBS-SIM"]

    def test_include_matches_on_instrument_name_or_mapped_value(self):
        tickets = [
            make_ticket(key="OBS-NAME", systems=("LSSTCam",)),
            make_ticket(key="OBS-MAPPED", systems=("Simonyi",)),
            make_ticket(key="OBS-NEITHER", systems=("AuxTel",)),
        ]
        result = filter_tickets_by_instrument(tickets, instrument="LSSTCam")
        assert {t["key"] for t in result} == {"OBS-NAME", "OBS-MAPPED"}

    def test_exclude_drops_matching_systems_and_keeps_the_rest(self):
        tickets = [
            make_ticket(key="OBS-SIM", systems=("Simonyi",)),
            make_ticket(key="OBS-AUX", systems=("AuxTel",)),
            make_ticket(key="OBS-NONE", systems=()),
        ]
        result = filter_tickets_by_instrument(tickets, instrument="LSSTCam", exclude=True)
        assert {t["key"] for t in result} == {"OBS-AUX", "OBS-NONE"}
