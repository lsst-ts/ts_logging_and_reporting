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
