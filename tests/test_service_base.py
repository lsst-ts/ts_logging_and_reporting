import pytest
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.web_app.service import Service


class StubAdapter:
    """Stands in for a CachedAdapter; returns canned per-dayobs data."""

    def __init__(self, name, payload):
        self.name = name
        self.payload = payload

    def fetch(self, start_dayobs, end_dayobs):
        return self.payload


class PassthroughService(Service):
    def handle_request(self, start_dayobs, end_dayobs):
        return self.collate_response(self.fetch_all(start_dayobs, end_dayobs))

    def collate_response(self, data):
        return {"results": [data[dayobs] for dayobs in sorted(data)]}


class TestService:
    def test_base_class_is_abstract(self):
        with pytest.raises(TypeError):
            Service(adapters={})

    def test_fetch_all_merges_adapters_per_dayobs(self):
        service = PassthroughService(
            adapters={
                "one": StubAdapter("one", {20250101: "a1", 20250102: "a2"}),
                "two": StubAdapter("two", {20250101: "b1", 20250102: "b2"}),
            }
        )
        merged = service.fetch_all(20250101, 20250102)
        assert merged == {
            20250101: {"one": "a1", "two": "b1"},
            20250102: {"one": "a2", "two": "b2"},
        }

    def test_handle_request_collates_sorted_by_dayobs(self):
        service = PassthroughService(
            adapters={"one": StubAdapter("one", {20250102: "later", 20250101: "earlier"})}
        )
        response = service.handle_request(20250101, 20250102)
        assert response == {"results": [{"one": "earlier"}, {"one": "later"}]}


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

    def test_success_returns_response(self):
        service = PassthroughService(adapters={"one": StubAdapter("one", {20250101: "x"})})
        assert service.handle(20250101, 20250101) == {"results": [{"one": "x"}]}
