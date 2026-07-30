import threading
import time

import lsst.ts.logging_and_reporting.refresh_worker as refresh_worker_module
from lsst.ts.logging_and_reporting.refresh_worker import RefreshWorker


class StubAdapter:
    """Records refresh() calls; optionally fails."""

    def __init__(self, name="stub", error=None):
        self.name = name
        self.error = error
        self.refreshed = []

    def refresh(self, dayobs):
        self.refreshed.append(dayobs)
        if self.error is not None:
            raise self.error


def fix_today(monkeypatch, dayobs):
    monkeypatch.setattr(refresh_worker_module, "current_dayobs", lambda: dayobs)


class TestRefreshCycle:
    def test_cycle_refreshes_today_on_all_adapters(self, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapters = [StubAdapter("a"), StubAdapter("b")]
        worker = RefreshWorker(adapters, interval_seconds=300)
        worker._refresh_cycle()
        assert adapters[0].refreshed == [20250101]
        assert adapters[1].refreshed == [20250101]

    def test_rollover_finalises_previous_dayobs(self, monkeypatch):
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        # Second cycle: one final refresh of 20250101, then 20250102.
        assert adapter.refreshed == [20250101, 20250101, 20250102]

    def test_no_finalisation_without_rollover(self, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=300)
        worker._refresh_cycle()
        worker._refresh_cycle()
        assert adapter.refreshed == [20250101, 20250101]

    def test_adapter_failure_does_not_abort_cycle(self, monkeypatch):
        fix_today(monkeypatch, 20250101)
        failing = StubAdapter("failing", error=RuntimeError("upstream down"))
        healthy = StubAdapter("healthy")
        worker = RefreshWorker([failing, healthy], interval_seconds=300)
        worker._refresh_cycle()
        assert failing.refreshed == [20250101]
        assert healthy.refreshed == [20250101]


class TestRunLoop:
    def test_immediate_first_cycle_and_periodic_repeat(self, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=0.05)
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 2.0
            while len(adapter.refreshed) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            worker.stop()
            thread.join(timeout=2.0)
        # First cycle ran immediately, further cycles on the interval.
        assert len(adapter.refreshed) >= 2

    def test_stop_ends_the_loop(self, monkeypatch):
        fix_today(monkeypatch, 20250101)
        worker = RefreshWorker([StubAdapter()], interval_seconds=0.05)
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        time.sleep(0.02)
        worker.stop()
        thread.join(timeout=2.0)
        assert not thread.is_alive()
