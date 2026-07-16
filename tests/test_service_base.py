import pytest

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
