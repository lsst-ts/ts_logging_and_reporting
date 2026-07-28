import threading

import pytest
import requests
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.services.base_service import Service


class StubAdapter:
    """Stands in for a DayobsCachedAdapter; returns canned per-dayobs data."""

    def __init__(self, name, payload):
        self.name = name
        self.payload = payload

    def fetch(self, start_dayobs, end_dayobs):
        return self.payload


class PassthroughService(Service):
    def handle_request(self, start_dayobs, end_dayobs):
        results = self.fetch_concurrently(
            {name: (lambda a=a: a.fetch(start_dayobs, end_dayobs)) for name, a in self.adapters.items()}
        )
        return self.collate_response(results)

    def collate_response(self, data):
        return {"results": [data[name] for name in sorted(data)]}


class TestService:
    def test_base_class_is_abstract(self):
        with pytest.raises(TypeError):
            Service(adapters={})

    def test_handle_request_collates_results_by_name(self):
        service = PassthroughService(
            adapters={"one": StubAdapter("one", {20250101: "x"}), "two": StubAdapter("two", {20250101: "y"})}
        )
        response = service.handle_request(20250101, 20250102)
        assert response == {"results": [{20250101: "x"}, {20250101: "y"}]}


class TestFetchConcurrently:
    def test_returns_each_result_by_name(self):
        service = PassthroughService(adapters={})
        assert service.fetch_concurrently({"a": lambda: 1, "b": lambda: 2}) == {"a": 1, "b": 2}

    def test_empty_tasks_returns_empty(self):
        service = PassthroughService(adapters={})
        assert service.fetch_concurrently({}) == {}

    def test_captures_exception_without_aborting_others(self):
        boom = RuntimeError("boom")

        def fail():
            raise boom

        results = PassthroughService(adapters={}).fetch_concurrently({"ok": lambda: "fine", "bad": fail})
        assert results["ok"] == "fine"
        assert results["bad"] is boom

    def test_tasks_run_concurrently(self):
        # Each task blocks on the barrier until the other arrives, so this
        # only completes if the two run at once rather than serially.
        barrier = threading.Barrier(2, timeout=5)

        def task():
            barrier.wait()
            return "done"

        results = PassthroughService(adapters={}).fetch_concurrently({"a": task, "b": task})
        assert results == {"a": "done", "b": "done"}


class FailingService(Service):
    def __init__(self, error):
        super().__init__(adapters={})
        self.error = error

    def handle_request(self):
        raise self.error

    def collate_response(self, data):
        return {}


class TestHandle:
    def test_unexpected_error_becomes_500(self):
        service = FailingService(ValueError("upstream exploded"))
        with pytest.raises(HTTPException) as exc_info:
            service.handle()
        assert exc_info.value.status_code == 500
        assert "upstream exploded" in exc_info.value.detail

    def test_http_exception_passes_through(self):
        service = FailingService(HTTPException(status_code=401, detail="no token"))
        with pytest.raises(HTTPException) as exc_info:
            service.handle()
        assert exc_info.value.status_code == 401

    def test_upstream_request_failure_becomes_502(self):
        service = FailingService(requests.ConnectionError("upstream unreachable"))
        with pytest.raises(HTTPException) as exc_info:
            service.handle()
        assert exc_info.value.status_code == 502
        assert "upstream unreachable" in exc_info.value.detail

    def test_success_returns_response(self):
        service = PassthroughService(adapters={"one": StubAdapter("one", {20250101: "x"})})
        assert service.handle(20250101, 20250101) == {"results": [{20250101: "x"}]}
