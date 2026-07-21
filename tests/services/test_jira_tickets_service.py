from lsst.ts.logging_and_reporting.services.jira import JiraTicketsService


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
    return JiraTicketsService(adapters={"jira_obs": adapter}), adapter


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
