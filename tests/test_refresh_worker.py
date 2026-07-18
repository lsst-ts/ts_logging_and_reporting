import time

import lsst.ts.logging_and_reporting.web_app.refresh_worker as refresh_worker_module
from lsst.ts.logging_and_reporting.web_app.refresh_worker import LEADER_LOCK_KEY, RefreshWorker


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
    def test_cycle_refreshes_today_on_all_adapters(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapters = [StubAdapter("a"), StubAdapter("b")]
        worker = RefreshWorker(adapters, fake_redis, interval_seconds=300)
        worker._refresh_cycle()
        assert adapters[0].refreshed == [20250101]
        assert adapters[1].refreshed == [20250101]

    def test_rollover_finalises_previous_dayobs(self, fake_redis, monkeypatch):
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], fake_redis, interval_seconds=300)
        fix_today(monkeypatch, 20250101)
        worker._refresh_cycle()
        fix_today(monkeypatch, 20250102)
        worker._refresh_cycle()
        # Second cycle: one final refresh of 20250101, then 20250102.
        assert adapter.refreshed == [20250101, 20250101, 20250102]

    def test_no_finalisation_without_rollover(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], fake_redis, interval_seconds=300)
        worker._refresh_cycle()
        worker._refresh_cycle()
        assert adapter.refreshed == [20250101, 20250101]

    def test_adapter_failure_does_not_abort_cycle(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        failing = StubAdapter("failing", error=RuntimeError("upstream down"))
        healthy = StubAdapter("healthy")
        worker = RefreshWorker([failing, healthy], fake_redis, interval_seconds=300)
        worker._refresh_cycle()
        assert failing.refreshed == [20250101]
        assert healthy.refreshed == [20250101]


class TestLeaderLease:
    def test_only_leader_refreshes(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter1, adapter2 = StubAdapter("a1"), StubAdapter("a2")
        leader = RefreshWorker([adapter1], fake_redis, interval_seconds=300)
        follower = RefreshWorker([adapter2], fake_redis, interval_seconds=300)
        leader._refresh_cycle()
        follower._refresh_cycle()
        assert adapter1.refreshed == [20250101]
        assert adapter2.refreshed == []

    def test_leader_renews_its_own_lease(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], fake_redis, interval_seconds=300)
        worker._refresh_cycle()
        worker._refresh_cycle()
        assert adapter.refreshed == [20250101, 20250101]

    def test_follower_takes_over_after_release(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter1, adapter2 = StubAdapter("a1"), StubAdapter("a2")
        leader = RefreshWorker([adapter1], fake_redis, interval_seconds=300)
        follower = RefreshWorker([adapter2], fake_redis, interval_seconds=300)
        leader._refresh_cycle()
        leader._release_leadership()
        follower._refresh_cycle()
        assert adapter2.refreshed == [20250101]

    def test_release_only_removes_own_lease(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        leader = RefreshWorker([StubAdapter()], fake_redis, interval_seconds=300)
        other = RefreshWorker([StubAdapter()], fake_redis, interval_seconds=300)
        leader._refresh_cycle()
        other._release_leadership()  # not the holder; must be a no-op
        assert fake_redis.exists(LEADER_LOCK_KEY) == 1


class TestThreadLifecycle:
    def test_immediate_first_cycle_and_periodic_repeat(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        adapter = StubAdapter()
        worker = RefreshWorker([adapter], fake_redis, interval_seconds=0.05)
        worker.start()
        try:
            deadline = time.monotonic() + 2.0
            while len(adapter.refreshed) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            worker.stop()
        # First cycle ran immediately, further cycles on the interval.
        assert len(adapter.refreshed) >= 2

    def test_stop_joins_thread_and_releases_lease(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        worker = RefreshWorker([StubAdapter()], fake_redis, interval_seconds=0.05)
        worker.start()
        time.sleep(0.02)
        worker.stop()
        assert worker._thread is None
        assert fake_redis.exists(LEADER_LOCK_KEY) == 0

    def test_start_twice_does_not_spawn_second_thread(self, fake_redis, monkeypatch):
        fix_today(monkeypatch, 20250101)
        worker = RefreshWorker([StubAdapter()], fake_redis, interval_seconds=60)
        worker.start()
        try:
            thread = worker._thread
            worker.start()
            assert worker._thread is thread
        finally:
            worker.stop()
