import pytest
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.web_app.services.block_details import (
    ZEPHYR_TEST_CASE_PATH,
    BlockDetailsService,
)

HOSTNAME = "jira.test"


class StubIdAdapter:
    """Stands in for an IdBasedAdapter; returns canned summaries."""

    def __init__(self, summaries=None, error=None):
        self.summaries = summaries or {}
        self.error = error
        self.requested = []

    def fetch_by_ids(self, ids):
        self.requested.append(list(ids))
        if self.error is not None:
            raise self.error
        return {key: self.summaries.get(key) for key in ids}


def make_service(zephyr=None, jira=None):
    return BlockDetailsService(
        adapters={
            "zephyr": zephyr if zephyr is not None else StubIdAdapter(),
            "jira": jira if jira is not None else StubIdAdapter(),
        }
    )


@pytest.fixture(autouse=True)
def jira_hostname(monkeypatch):
    monkeypatch.setenv("JIRA_API_HOSTNAME", HOSTNAME)


class TestKeyRouting:
    def test_keys_split_by_pattern(self):
        zephyr = StubIdAdapter({"BLOCK-T1": "Case"})
        jira = StubIdAdapter({"BLOCK-2": "Ticket"})
        make_service(zephyr, jira).handle_request(["BLOCK-T1", "BLOCK-2"])
        assert zephyr.requested == [["BLOCK-T1"]]
        assert jira.requested == [["BLOCK-2"]]

    def test_duplicates_and_unmatched_keys_dropped(self):
        zephyr = StubIdAdapter({"BLOCK-T1": "Case"})
        jira = StubIdAdapter()
        response = make_service(zephyr, jira).handle_request(
            ["BLOCK-T1", "BLOCK-T1", "JUNK-1", "BLOCKT2", "BLOCK-T3_"]
        )
        assert zephyr.requested == [["BLOCK-T1"]]
        assert jira.requested == []
        assert list(response["data"]) == ["BLOCK-T1"]

    def test_sources_with_no_keys_are_not_queried(self):
        zephyr = StubIdAdapter()
        jira = StubIdAdapter()
        response = make_service(zephyr, jira).handle_request(["NOT-A-BLOCK"])
        assert zephyr.requested == []
        assert jira.requested == []
        assert response == {"data": {}, "errors": {}}


class TestResponseShape:
    def test_records_carry_source_and_url(self):
        service = make_service(
            zephyr=StubIdAdapter({"BLOCK-T123_a": "Zephyr case"}),
            jira=StubIdAdapter({"BLOCK-456": "Jira block"}),
        )
        response = service.handle_request(["BLOCK-T123_a", "BLOCK-456"])
        assert response == {
            "data": {
                "BLOCK-T123_a": {
                    "key": "BLOCK-T123_a",
                    "summary": "Zephyr case",
                    "source": "zephyr",
                    "url": f"https://{HOSTNAME}{ZEPHYR_TEST_CASE_PATH}BLOCK-T123",
                },
                "BLOCK-456": {
                    "key": "BLOCK-456",
                    "summary": "Jira block",
                    "source": "jira",
                    "url": f"https://{HOSTNAME}/browse/BLOCK-456",
                },
            },
            "errors": {},
        }

    def test_unresolved_keys_omitted(self):
        service = make_service(zephyr=StubIdAdapter({"BLOCK-T1": "Case"}))
        response = service.handle_request(["BLOCK-T1", "BLOCK-T404"])
        assert list(response["data"]) == ["BLOCK-T1"]
        assert response["errors"] == {}


class TestErrorReporting:
    def test_one_source_failing_reports_error_and_keeps_other(self):
        service = make_service(
            zephyr=StubIdAdapter(error=ConnectionError("Zephyr down")),
            jira=StubIdAdapter({"BLOCK-2": "Ticket"}),
        )
        response = service.handle_request(["BLOCK-T1", "BLOCK-2"])
        assert response["errors"] == {"zephyr": "Zephyr down"}
        assert list(response["data"]) == ["BLOCK-2"]

    def test_both_sources_failing_raises_500(self):
        service = make_service(
            zephyr=StubIdAdapter(error=ConnectionError("Zephyr down")),
            jira=StubIdAdapter(error=ConnectionError("Jira down")),
        )
        with pytest.raises(HTTPException) as exc_info:
            service.handle_request(["BLOCK-T1", "BLOCK-2"])
        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Both Zephyr and Jira requests failed."

    def test_failing_source_not_reported_when_it_had_no_keys(self):
        service = make_service(
            zephyr=StubIdAdapter(error=ConnectionError("Zephyr down")),
            jira=StubIdAdapter({"BLOCK-2": "Ticket"}),
        )
        response = service.handle_request(["BLOCK-2"])
        assert response["errors"] == {}

    def test_http_exception_propagates(self):
        service = make_service(
            zephyr=StubIdAdapter(error=HTTPException(status_code=401, detail="no token")),
            jira=StubIdAdapter({"BLOCK-2": "Ticket"}),
        )
        with pytest.raises(HTTPException) as exc_info:
            service.handle_request(["BLOCK-T1", "BLOCK-2"])
        assert exc_info.value.status_code == 401
