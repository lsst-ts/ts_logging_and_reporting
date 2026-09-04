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

from lsst.ts.logging_and_reporting.adapters.rubin_nights_context import CONTEXT_FEED_COLS
from lsst.ts.logging_and_reporting.services.context_feed import ContextFeedService


def row(time, final_status=None, name=None, **extra):
    record = {"time": time, "finalStatus": final_status, "name": name}
    record.update(extra)
    return record


def task_change(time, name):
    """A task-change row, whose start is distinct from its time."""
    return row(
        time,
        "Task Change",
        name=name,
        timestampProcessStart=f"{time}-start",
        timestampProcessEnd="stale",
    )


class StubContextAdapter:
    def __init__(self, buckets):
        self.buckets = buckets
        self.fetch_calls = []

    def fetch(self, start_dayobs, end_dayobs):
        self.fetch_calls.append((start_dayobs, end_dayobs))
        return self.buckets


def make_service(buckets=None, context_adapter=None):
    return ContextFeedService(context_adapter=context_adapter or StubContextAdapter(buckets or {}))


class TestHandleRequest:
    def test_fetches_inclusive_range(self):
        adapter = StubContextAdapter({})
        service = make_service(context_adapter=adapter)
        service.handle_request(20250101, 20250103)
        assert adapter.fetch_calls == [(20250101, 20250103)]

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
                task_change("t0", "BLOCK-1"),
                row("t1"),
                task_change("t2", "BLOCK-2"),
                row("t3"),
            ]
        }
        records = service.collate_response(buckets)
        # First task change spans to the next one's start.
        assert records[0]["timestampProcessEnd"] == "t2-start"
        # The final task change spans to the last message in the range.
        assert records[2]["timestampProcessEnd"] == "t3"

    def test_task_change_chains_across_dayobs_buckets(self):
        service = make_service()
        buckets = {
            20250101: [task_change("t0", "BLOCK-1")],
            20250102: [task_change("t2", "BLOCK-2")],
        }
        records = service.collate_response(buckets)
        assert records[0]["timestampProcessEnd"] == "t2-start"
        assert records[1]["timestampProcessEnd"] == "t2"

    def test_task_change_end_keeps_the_unshifted_instant(self):
        # A task-change row's time is a nanosecond behind the instant it
        # announces; timestampProcessStart holds the unshifted value.
        service = make_service()
        buckets = {
            20250101: [
                row(
                    "2025-08-03T12:00:00.290645999+00:00",
                    "Task Change",
                    name="BLOCK-1",
                    timestampProcessStart="2025-08-03T12:00:00.290646+00:00",
                    timestampProcessEnd="stale",
                ),
                row(
                    "2025-08-03T13:00:00.499999999+00:00",
                    "Task Change",
                    name="BLOCK-2",
                    timestampProcessStart="2025-08-03T13:00:00.500000+00:00",
                    timestampProcessEnd="stale",
                ),
                row("2025-08-03T14:00:00.123456+00:00"),
            ]
        }
        records = service.collate_response(buckets)
        assert records[0]["timestampProcessEnd"] == "2025-08-03T13:00:00.500000+00:00"

    def test_non_task_change_end_untouched(self):
        service = make_service()
        buckets = {20250101: [row("t0", None, timestampProcessEnd="keep")]}
        assert service.collate_response(buckets)[0]["timestampProcessEnd"] == "keep"

    def test_no_task_changes_is_noop(self):
        service = make_service()
        buckets = {20250101: [row("t0"), row("t1")]}
        records = service.collate_response(buckets)
        assert [record["time"] for record in records] == ["t0", "t1"]
