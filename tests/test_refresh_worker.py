import logging
import threading
import time

import pytest

import lsst.ts.logging_and_reporting.refresh_worker as refresh_worker_module
from lsst.ts.logging_and_reporting.refresh_worker import RefreshWorker


class StubAdapter:
    """Records refresh() calls; optionally fails."""

    def __init__(self, name="stub", error=None, on_refresh=None):
        self.name = name
        self.error = error
        self.on_refresh = on_refresh
        self.refreshed = []

    def refresh(self, dayobs):
        self.refreshed.append(dayobs)
        if self.on_refresh is not None:
            self.on_refresh()
        if self.error is not None:
            raise self.error


class FakeClock:
    """Stands in for the module's ``time``, returning canned readings.

    Each `monotonic` call consumes the next reading, so a test can fix
    exactly how long a cycle appears to take.
    """

    def __init__(self, *readings):
        self._readings = list(readings)

    def monotonic(self):
        return self._readings.pop(0)


def fix_today(monkeypatch, dayobs):
    monkeypatch.setattr(refresh_worker_module, "current_dayobs", lambda: dayobs)


def messages_at(caplog, level):
    """The worker's log lines at ``level``, in order."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == refresh_worker_module.__name__ and record.levelno == level
    ]


def cycle_log(caplog):
    """The worker's INFO cycle lines, in order."""
    return messages_at(caplog, logging.INFO)


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


class TestRefreshAllCounts:
    def test_counts_successes_and_failures(self):
        adapters = [
            StubAdapter("a"),
            StubAdapter("failing", error=RuntimeError("upstream down")),
            StubAdapter("b"),
        ]
        worker = RefreshWorker(adapters, interval_seconds=300)
        assert worker._refresh_all(20250101) == (2, 1)

    def test_all_failing(self):
        adapters = [StubAdapter(str(i), error=RuntimeError("boom")) for i in range(3)]
        worker = RefreshWorker(adapters, interval_seconds=300)
        assert worker._refresh_all(20250101) == (0, 3)

    def test_no_adapters(self):
        worker = RefreshWorker([], interval_seconds=300)
        assert worker._refresh_all(20250101) == (0, 0)

    def test_stop_before_the_pass_refreshes_nothing(self):
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=300)
        worker.stop()
        # Adapters skipped for shutdown count as neither outcome.
        assert worker._refresh_all(20250101) == (0, 0)
        assert adapter.refreshed == []

    def test_stop_mid_pass_skips_remaining_adapters(self):
        worker = RefreshWorker([], interval_seconds=300)
        first = StubAdapter("first", on_refresh=worker.stop)
        second = StubAdapter("second")
        worker._adapters = [first, second]
        # The completed refresh still counts; the skipped one does not.
        assert worker._refresh_all(20250101) == (1, 0)
        assert second.refreshed == []


class TestCycleLogging:
    @pytest.fixture(autouse=True)
    def capture_info(self, caplog):
        caplog.set_level(logging.INFO, logger=refresh_worker_module.__name__)

    def test_start_and_finish_are_logged_with_counts(self, monkeypatch, caplog):
        fix_today(monkeypatch, 20250101)
        monkeypatch.setattr(refresh_worker_module, "time", FakeClock(100.0, 102.5))
        adapters = [
            StubAdapter("a"),
            StubAdapter("b"),
            StubAdapter("failing", error=RuntimeError("upstream down")),
        ]
        RefreshWorker(adapters, interval_seconds=300)._refresh_cycle()
        messages = cycle_log(caplog)
        assert messages[0] == "RefreshWorker: refresh cycle started"
        assert messages[-1] == "RefreshWorker: refresh cycle finished in 2.5s (2 succeeded, 1 failed)"

    def test_rollover_cycle_counts_both_passes(self, monkeypatch, caplog):
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        caplog.clear()
        fix_today(monkeypatch, 20250102)
        monkeypatch.setattr(refresh_worker_module, "time", FakeClock(0.0, 1.0))
        worker._refresh_cycle()
        # Finalising 20250101 plus refreshing 20250102: two successes.
        assert (
            cycle_log(caplog)[-1] == "RefreshWorker: refresh cycle finished in 1.0s (2 succeeded, 0 failed)"
        )

    def test_finish_is_logged_when_the_cycle_itself_fails(self, monkeypatch, caplog):
        def boom():
            raise RuntimeError("clock unavailable")

        monkeypatch.setattr(refresh_worker_module, "current_dayobs", boom)
        monkeypatch.setattr(refresh_worker_module, "time", FakeClock(0.0, 0.3))
        RefreshWorker([StubAdapter()], interval_seconds=300)._refresh_cycle()
        messages = cycle_log(caplog)
        assert messages[0] == "RefreshWorker: refresh cycle started"
        assert messages[-1] == "RefreshWorker: refresh cycle finished in 0.3s (0 succeeded, 0 failed)"


class TestSlowCycleWarning:
    @pytest.fixture(autouse=True)
    def capture_info(self, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger=refresh_worker_module.__name__)
        fix_today(monkeypatch, 20250101)

    def run_cycle(self, monkeypatch, elapsed, interval=300):
        monkeypatch.setattr(refresh_worker_module, "time", FakeClock(0.0, elapsed))
        RefreshWorker([StubAdapter()], interval_seconds=interval)._refresh_cycle()

    def test_slow_cycle_warns(self, monkeypatch, caplog):
        self.run_cycle(monkeypatch, elapsed=200.0)
        assert messages_at(caplog, logging.WARNING) == [
            "RefreshWorker: cycle took 200.0s, over 50% of the 300s interval"
        ]

    def test_fast_cycle_does_not_warn(self, monkeypatch, caplog):
        self.run_cycle(monkeypatch, elapsed=100.0)
        assert messages_at(caplog, logging.WARNING) == []

    def test_exactly_half_the_interval_does_not_warn(self, monkeypatch, caplog):
        self.run_cycle(monkeypatch, elapsed=150.0)
        assert messages_at(caplog, logging.WARNING) == []

    def test_warning_is_relative_to_the_configured_interval(self, monkeypatch, caplog):
        # The same duration is fine at 300s but slow at a 60s interval.
        self.run_cycle(monkeypatch, elapsed=40.0, interval=60)
        assert messages_at(caplog, logging.WARNING) == [
            "RefreshWorker: cycle took 40.0s, over 50% of the 60s interval"
        ]

    def test_slow_cycle_still_logs_the_finish_line(self, monkeypatch, caplog):
        self.run_cycle(monkeypatch, elapsed=200.0)
        assert (
            cycle_log(caplog)[-1] == "RefreshWorker: refresh cycle finished in 200.0s (1 succeeded, 0 failed)"
        )


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
