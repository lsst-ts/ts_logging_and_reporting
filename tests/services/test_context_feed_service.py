from lsst.ts.logging_and_reporting.adapters.rubin_nights_context import CONTEXT_FEED_COLS
from lsst.ts.logging_and_reporting.services.context_feed import ContextFeedService


def row(time, final_status=None, name=None, **extra):
    record = {"time": time, "finalStatus": final_status, "name": name}
    record.update(extra)
    return record


class StubContextAdapter:
    def __init__(self, buckets):
        self.buckets = buckets
        self.fetch_calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.fetch_calls.append((start_dayobs, end_dayobs))
        return self.buckets


def make_service(buckets=None):
    return ContextFeedService(adapters={"context": StubContextAdapter(buckets or {})})


class TestHandleRequest:
    def test_fetches_inclusive_range(self):
        service = make_service()
        service.handle_request(20250101, 20250103)
        assert service.adapters["context"].fetch_calls == [(20250101, 20250103)]

    def test_returns_data_and_constant_cols(self):
        service = make_service(buckets={20250101: [row("t0", name="a")]})
        response = service.handle_request(20250101, 20250101)
        assert response["cols"] == CONTEXT_FEED_COLS
        assert [record["name"] for record in response["data"]] == ["a"]


class TestCollateResponse:
    def test_flattens_in_dayobs_order(self):
        service = make_service()
        buckets = {20250102: [row("t2", name="b")], 20250101: [row("t0", name="a")]}
        assert [record["name"] for record in service.collate_response(buckets)] == ["a", "b"]

    def test_empty(self):
        assert make_service().collate_response({}) == []

    def test_task_change_end_chains_to_next(self):
        service = make_service()
        buckets = {
            20250101: [
                row("t0", "Task Change", name="BLOCK-1", timestampProcessEnd="stale"),
                row("t1"),
                row("t2", "Task Change", name="BLOCK-2", timestampProcessEnd="stale"),
                row("t3"),
            ]
        }
        records = service.collate_response(buckets)
        # First task change spans to the next one's start.
        assert records[0]["timestampProcessEnd"] == "t2"
        # The final task change spans to the last message in the range.
        assert records[2]["timestampProcessEnd"] == "t3"

    def test_task_change_chains_across_dayobs_buckets(self):
        service = make_service()
        buckets = {
            20250101: [row("t0", "Task Change", name="BLOCK-1", timestampProcessEnd="stale")],
            20250102: [row("t2", "Task Change", name="BLOCK-2", timestampProcessEnd="stale")],
        }
        records = service.collate_response(buckets)
        assert records[0]["timestampProcessEnd"] == "t2"
        assert records[1]["timestampProcessEnd"] == "t2"

    def test_non_task_change_end_untouched(self):
        service = make_service()
        buckets = {20250101: [row("t0", None, timestampProcessEnd="keep")]}
        assert service.collate_response(buckets)[0]["timestampProcessEnd"] == "keep"

    def test_no_task_changes_is_noop(self):
        service = make_service()
        buckets = {20250101: [row("t0"), row("t1")]}
        records = service.collate_response(buckets)
        assert [record["time"] for record in records] == ["t0", "t1"]
