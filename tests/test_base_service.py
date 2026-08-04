import logging
import threading

import pytest
import requests
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.logging_config import (
    NO_TRACE_ID,
    current_trace_id,
    set_trace_id,
)


class StubAdapter:
    """Stands in for a DayobsCachedAdapter; returns canned per-dayobs data."""

    def __init__(self, name, payload):
        self.name = name
        self.payload = payload

    def fetch(self, start_dayobs, end_dayobs):
        return self.payload


class PassthroughService(Service):
    def __init__(self, adapters):
        self.adapters = adapters

    def handle(self, start_dayobs, end_dayobs):
        results = self.fetch_concurrently(
            {name: (lambda a=a: a.fetch(start_dayobs, end_dayobs)) for name, a in self.adapters.items()}
        )
        return self.collate_response(results)

    def collate_response(self, data):
        return {"results": [data[name] for name in sorted(data)]}


class TestService:
    def test_base_class_is_abstract(self):
        with pytest.raises(TypeError):
            Service()

    def test_handle_collates_results_by_name(self):
        service = PassthroughService(
            adapters={
                "one": StubAdapter("one", {20250101: "x"}),
                "two": StubAdapter("two", {20250101: "y"}),
            }
        )
        response = service.handle(20250101, 20250102)
        assert response == {"results": [{20250101: "x"}, {20250101: "y"}]}


class TestFetchConcurrently:
    def test_returns_each_result_by_name(self):
        service = PassthroughService(adapters={})
        assert service.fetch_concurrently({"a": lambda: 1, "b": lambda: 2}) == {
            "a": 1,
            "b": 2,
        }

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


class TestTraceIdPropagation:
    """Each fetch thread logs under the request that started it."""

    @pytest.fixture(autouse=True)
    def clear_trace_id(self):
        yield
        set_trace_id(NO_TRACE_ID)

    def test_tasks_see_the_trace_id(self):
        set_trace_id("abc12345")
        results = PassthroughService(adapters={}).fetch_concurrently(
            {"a": current_trace_id, "b": current_trace_id}
        )
        assert results == {"a": "abc12345", "b": "abc12345"}

    def test_tasks_outside_a_request_see_the_marker(self):
        results = PassthroughService(adapters={}).fetch_concurrently({"a": current_trace_id})
        assert results == {"a": NO_TRACE_ID}

    def test_overlapping_tasks_each_get_their_own_context(self):
        # One shared Context cannot be entered by two threads at once,
        # so a single copy for all tasks would fail them here.
        set_trace_id("abc12345")
        barrier = threading.Barrier(3, timeout=5)

        def task():
            barrier.wait()
            return current_trace_id()

        results = PassthroughService(adapters={}).fetch_concurrently({"a": task, "b": task, "c": task})
        assert results == {"a": "abc12345", "b": "abc12345", "c": "abc12345"}

    def test_a_task_setting_the_id_does_not_affect_the_caller(self):
        set_trace_id("abc12345")
        PassthroughService(adapters={}).fetch_concurrently({"a": lambda: set_trace_id("overwritten")})
        assert current_trace_id() == "abc12345"


class FailingService(Service):
    def __init__(self, error):
        self.error = error

    def handle(self):
        raise self.error

    def collate_response(self, data):
        return {}


class TestHandleRequest:
    def test_unexpected_error_becomes_500(self, caplog):
        service = FailingService(ValueError("Service exploded"))
        with caplog.at_level(logging.ERROR):
            with pytest.raises(HTTPException) as exc_info:
                service.handle_request()
        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Internal error in FailingService"
        assert "Service exploded" in caplog.text

    def test_http_exception_passes_through(self):
        service = FailingService(HTTPException(status_code=404, detail="No simulation for 20240101"))
        with pytest.raises(HTTPException) as exc_info:
            service.handle_request()
        assert exc_info.value.status_code == 404
        # Verbatim, not rewrapped as "Internal error in FailingService".
        assert exc_info.value.detail == "No simulation for 20240101"

    def test_upstream_request_failure_becomes_502(self, caplog):
        service = FailingService(requests.ConnectionError("upstream unreachable"))
        with caplog.at_level(logging.ERROR):
            with pytest.raises(HTTPException) as exc_info:
                service.handle_request()
        assert exc_info.value.status_code == 502
        assert exc_info.value.detail == "Upstream failure in FailingService"
        assert "upstream unreachable" in caplog.text

    def test_success_returns_response(self):
        service = PassthroughService(adapters={"one": StubAdapter("one", {20250101: "x"})})
        assert service.handle_request(20250101, 20250101) == {"results": [{20250101: "x"}]}
