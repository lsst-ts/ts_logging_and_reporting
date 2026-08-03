import json
import logging
import threading
import time

import pytest

import lsst.ts.logging_and_reporting.adapters.base_adapters as base_adapters_module
import lsst.ts.logging_and_reporting.refresh_worker as refresh_worker_module
from lsst.ts.logging_and_reporting.adapters.base_adapters import (
    DayobsCachedAdapter,
    InstrumentDayobsCachedAdapter,
)
from lsst.ts.logging_and_reporting.adapters.mixins import MutableDataMixin
from lsst.ts.logging_and_reporting.cache_ttl import (
    HISTORIC_TTL_REDIS,
    MUTABLE_TTL_REDIS,
    TODAY_TTL_REDIS,
)
from lsst.ts.logging_and_reporting.refresh_worker import RefreshWorker
from lsst.ts.logging_and_reporting.utils.dayobs import dayobs_range


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


class RecordingEvent:
    """Stands in for the worker's stop event.

    Records the timeout of every `wait` and reports "stopped" once
    ``cycles`` of them have gone by, so `RefreshWorker.run` returns
    after a known number of cycles and the test can inspect how long
    it meant to sleep between them.
    """

    def __init__(self, cycles):
        self.waits = []
        self._remaining = cycles
        self._flag = False

    def wait(self, timeout=None):
        self.waits.append(timeout)
        self._remaining -= 1
        if self._remaining <= 0:
            self._flag = True
        return self._flag

    def is_set(self):
        return self._flag

    def set(self):
        self._flag = True


class CachingAdapter(DayobsCachedAdapter):
    """Real cache base over `FakeRedis`, so TTL policy is exercised.

    Each fetch returns a distinct value, letting a test tell a fresh
    refresh apart from the value stored on an earlier cycle.
    """

    name = "caching"

    def __init__(self, redis):
        super().__init__(redis)
        self.fetches = 0

    def _fetch_run(self, run_start, run_end):
        self.fetches += 1
        return {
            dayobs: f"{dayobs}-fetch-{self.fetches}"
            for dayobs in dayobs_range(run_start, run_end)
        }


class MutableCachingAdapter(MutableDataMixin, CachingAdapter):
    pass


class CachingInstrumentAdapter(InstrumentDayobsCachedAdapter):
    """Composite-key counterpart to `CachingAdapter`."""

    name = "caching_instrument"
    INSTRUMENTS = ("lsstcam", "latiss")

    def _fetch_run(self, instrument, run_start, run_end):
        return {
            dayobs: [{"day_obs": dayobs}] for dayobs in dayobs_range(run_start, run_end)
        }


def fix_today(monkeypatch, dayobs):
    """Move "today" for both the worker and the adapters' TTL policy."""
    monkeypatch.setattr(refresh_worker_module, "current_dayobs", lambda: dayobs)
    monkeypatch.setattr(base_adapters_module, "current_dayobs", lambda: dayobs)


def cached(fake_redis, key):
    return json.loads(fake_redis.get(key))


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

    def test_backwards_dayobs_does_not_finalise(self, monkeypatch):
        # A clock stepping backwards is not a rollover: 20250102 has
        # not finished, so finalising it would store a partial night
        # under the historical TTL.
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        assert adapter.refreshed == [20250102, 20250101]

    def test_rollover_after_a_backwards_step_still_finalises(self, monkeypatch):
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        assert adapter.refreshed == [20250102, 20250101, 20250101, 20250102]

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


class TestRolloverStorage:
    """What the finalisation pass actually leaves in the cache.

    The stub-based cycle tests see only which dayobs were refreshed;
    these run the worker against a real cache base over `FakeRedis`,
    where the point of the pass — the completed night re-stored under
    a long TTL — is visible.
    """

    def test_finalised_dayobs_is_refetched_under_the_historical_ttl(
        self, monkeypatch, fake_redis
    ):
        adapter = CachingAdapter(fake_redis)
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        assert fake_redis.ttls["adapter:caching:20250101"] == TODAY_TTL_REDIS
        assert cached(fake_redis, "adapter:caching:20250101") == "20250101-fetch-1"

        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        # Re-fetched after the night closed, not left at the value
        # stored by the last pre-rollover cycle, and now long-lived.
        assert fake_redis.ttls["adapter:caching:20250101"] == HISTORIC_TTL_REDIS
        assert cached(fake_redis, "adapter:caching:20250101") == "20250101-fetch-2"

    def test_the_new_today_keeps_the_short_ttl(self, monkeypatch, fake_redis):
        adapter = CachingAdapter(fake_redis)
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        assert fake_redis.ttls["adapter:caching:20250102"] == TODAY_TTL_REDIS

    def test_mutable_adapters_finalise_under_the_mutable_ttl(
        self, monkeypatch, fake_redis
    ):
        # A closed night's log messages can still be edited, so the
        # finalisation pass must not grant them the historical TTL.
        adapter = MutableCachingAdapter(fake_redis)
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        assert fake_redis.ttls["adapter:caching:20250101"] == MUTABLE_TTL_REDIS
        assert cached(fake_redis, "adapter:caching:20250101") == "20250101-fetch-2"

    def test_instrument_keys_are_finalised_per_instrument(
        self, monkeypatch, fake_redis
    ):
        adapter = CachingInstrumentAdapter(fake_redis)
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        for instrument in adapter.INSTRUMENTS:
            key = f"adapter:caching_instrument:{instrument}:20250101"
            assert fake_redis.ttls[key] == HISTORIC_TTL_REDIS
            assert cached(fake_redis, key) == [{"day_obs": 20250101}]

    def test_without_rollover_today_is_never_given_a_long_ttl(
        self, monkeypatch, fake_redis
    ):
        adapter = CachingAdapter(fake_redis)
        worker = RefreshWorker([adapter], interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        worker._refresh_cycle()
        assert fake_redis.ttls["adapter:caching:20250101"] == TODAY_TTL_REDIS


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
        assert (
            messages[-1]
            == "RefreshWorker: refresh cycle finished in 2.5s (2 succeeded, 1 failed)"
        )

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
            cycle_log(caplog)[-1]
            == "RefreshWorker: refresh cycle finished in 1.0s (2 succeeded, 0 failed)"
        )

    def test_finish_is_logged_when_the_cycle_itself_fails(self, monkeypatch, caplog):
        def boom():
            raise RuntimeError("clock unavailable")

        monkeypatch.setattr(refresh_worker_module, "current_dayobs", boom)
        monkeypatch.setattr(refresh_worker_module, "time", FakeClock(0.0, 0.3))
        RefreshWorker([StubAdapter()], interval_seconds=300)._refresh_cycle()
        messages = cycle_log(caplog)
        assert messages[0] == "RefreshWorker: refresh cycle started"
        assert (
            messages[-1]
            == "RefreshWorker: refresh cycle finished in 0.3s (0 succeeded, 0 failed)"
        )


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
            cycle_log(caplog)[-1]
            == "RefreshWorker: refresh cycle finished in 200.0s (1 succeeded, 0 failed)"
        )


class TestCycleScheduling:
    """Cycles start one interval apart, not one interval after the
    previous one finished."""

    def run_loop(self, monkeypatch, readings, cycles, interval=300, adapters=None):
        fix_today(monkeypatch, 20250101)
        monkeypatch.setattr(refresh_worker_module, "time", FakeClock(*readings))
        worker = RefreshWorker(adapters or [StubAdapter()], interval_seconds=interval)
        event = RecordingEvent(cycles)
        worker._stop_event = event
        worker.run()
        return event

    def test_wait_deducts_the_cycle_duration(self, monkeypatch):
        # A 50s cycle then a 100s one, against a 300s interval.
        event = self.run_loop(monkeypatch, [0.0, 50.0, 100.0, 200.0], cycles=2)
        assert event.waits == [250.0, 200.0]

    def test_overrunning_cycle_leaves_no_wait(self, monkeypatch):
        event = self.run_loop(monkeypatch, [0.0, 400.0], cycles=1)
        assert event.waits == [0.0]

    def test_schedule_catches_up_after_an_overrun(self, monkeypatch):
        # 400s cycle (no wait), then a 10s one back on schedule.
        event = self.run_loop(monkeypatch, [0.0, 400.0, 500.0, 510.0], cycles=2)
        assert event.waits == [0.0, 290.0]

    def test_first_cycle_runs_before_any_wait(self, monkeypatch):
        seen = []
        adapter = StubAdapter(on_refresh=lambda: seen.append("refreshed"))
        event = self.run_loop(monkeypatch, [0.0, 10.0], cycles=1, adapters=[adapter])
        assert seen == ["refreshed"]
        # A 290s wait can only have been computed from a finished 10s
        # cycle, so the refresh preceded the loop's first wait.
        assert event.waits == [290.0]


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
