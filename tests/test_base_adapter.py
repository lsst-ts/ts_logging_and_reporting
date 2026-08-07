import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from lsst.ts.logging_and_reporting.adapters import base_adapters
from lsst.ts.logging_and_reporting.adapters.base_adapters import (
    CachedAdapter,
    DayobsCachedAdapter,
    IdCachedAdapter,
    InstrumentDayobsCachedAdapter,
)
from lsst.ts.logging_and_reporting.adapters.mixins import MutableDataMixin
from lsst.ts.logging_and_reporting.cache_ttl import (
    HISTORIC_TTL_REDIS,
    MUTABLE_TTL_REDIS,
    TODAY_TTL_REDIS,
)
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    current_dayobs,
    dayobs_range,
)
from lsst.ts.logging_and_reporting.utils.logging_config import (
    current_trace_id,
    set_trace_id,
)

TODAY = current_dayobs()


class RecordingAdapter(DayobsCachedAdapter):
    """DayobsCachedAdapter whose upstream returns f"data-{dayobs}" per day."""

    name = "recording"
    POLL_INTERVAL = 0.005

    def __init__(self, redis, delay=0.0, error=None, on_fetch=None):
        super().__init__(redis)
        self.calls = []
        # The thread each _fetch_run ran on, in call order: what
        # distinguishes a run fetched inline from one fanned out.
        self.call_threads = []
        self.delay = delay
        self.error = error
        # Called while this request is "upstream", i.e. holding the
        # locks it won: the window another request collides with.
        self.on_fetch = on_fetch

    def _fetch_run(self, run_start, run_end):
        self.calls.append((run_start, run_end))
        self.call_threads.append(threading.current_thread())
        if self.on_fetch is not None:
            self.on_fetch(run_start, run_end)
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return {dayobs: f"data-{dayobs}" for dayobs in dayobs_range(run_start, run_end)}


def configured_adapter(fake_redis, adapter_class=RecordingAdapter, **overrides):
    """Build an adapter subclass with `MAX_PARALLEL_RUNS` set.

    Mirrors how a real adapter tunes it — as a class attribute on the
    subclass — rather than patching an instance.
    """
    knobs = {knob: overrides.pop(knob) for knob in ("MAX_PARALLEL_RUNS",) if knob in overrides}
    subclass = type(f"Configured{adapter_class.__name__}", (adapter_class,), knobs)
    return subclass(fake_redis, **overrides)


def prime(adapter, *dayobs_list):
    """Cache each dayobs on its own, then forget the calls that did it.

    Priming days individually is what leaves the cache fragmented, so
    the next request's misses form more than one run.
    """
    for dayobs in dayobs_list:
        adapter.fetch(dayobs, dayobs)
    adapter.calls.clear()
    adapter.call_threads.clear()


class ConcurrencyProbe:
    """An ``on_fetch`` hook recording how many runs overlap.

    Each call holds its slot until ``group_size`` runs are inside the
    hook together, so the overlap is forced rather than waited out: a
    serial implementation never fills the group and breaks the barrier
    instead of passing on a lucky schedule. ``max_active`` is the
    high-water mark, which the cap must hold down.
    """

    def __init__(self, group_size, timeout=5):
        self._barrier = threading.Barrier(group_size, timeout=timeout)
        self._mutex = threading.Lock()
        self.active = 0
        self.max_active = 0

    def __call__(self, *_):
        with self._mutex:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        self._barrier.wait()
        with self._mutex:
            self.active -= 1


class RecordingIdAdapter(IdCachedAdapter):
    name = "recording_ids"
    POLL_INTERVAL = 0.005

    def __init__(self, redis, delay=0.0):
        super().__init__(redis)
        self.calls = []
        self.delay = delay

    def _fetch_from_source(self, ids):
        self.calls.append(list(ids))
        if self.delay:
            time.sleep(self.delay)
        return {id_: f"detail-{id_}" for id_ in ids}


class RecordingInstrumentAdapter(InstrumentDayobsCachedAdapter):
    """InstrumentDayobsCachedAdapter returning one row per dayobs."""

    name = "recording_instrument"
    POLL_INTERVAL = 0.005

    def __init__(self, redis):
        super().__init__(redis)
        self.calls = []

    def _fetch_run(self, instrument, run_start, run_end):
        self.calls.append((instrument, run_start, run_end))
        return {dayobs: [{"day_obs": dayobs}] for dayobs in dayobs_range(run_start, run_end)}


class TestCacheLoop:
    def test_cold_fetches_all_and_stores(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        result = adapter.fetch(20250101, 20250103)
        assert result == {d: f"data-{d}" for d in dayobs_range(20250101, 20250103)}
        assert adapter.calls == [(20250101, 20250103)]
        assert fake_redis.get("adapter:recording:20250102") is not None

    def test_hot_never_contacts_source(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.fetch(20250101, 20250103)
        adapter.calls.clear()
        result = adapter.fetch(20250101, 20250103)
        assert result == {d: f"data-{d}" for d in dayobs_range(20250101, 20250103)}
        assert adapter.calls == []

    def test_partial_fetches_only_misses(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.fetch(20250102, 20250102)
        adapter.calls.clear()
        adapter.fetch(20250101, 20250103)
        # The cached 20250102 splits the misses into two single-day
        # runs, which are fetched concurrently, so in no fixed order.
        assert sorted(adapter.calls) == [(20250101, 20250101), (20250103, 20250103)]

    def test_values_json_roundtrip(self, fake_redis):
        class StructuredAdapter(RecordingAdapter):
            def _fetch_run(self, run_start, run_end):
                self.calls.append((run_start, run_end))
                return {d: {"n": d, "items": [1, 2], "empty": None} for d in dayobs_range(run_start, run_end)}

        adapter = StructuredAdapter(fake_redis)
        first = adapter.fetch(20250101, 20250101)
        cached = adapter.fetch(20250101, 20250101)
        assert cached == first
        assert len(adapter.calls) == 1

    def test_non_ascii_stored_as_utf8(self, fake_redis):
        text = "日本語 中文 😀"

        class UnicodeAdapter(RecordingAdapter):
            def _fetch_run(self, run_start, run_end):
                self.calls.append((run_start, run_end))
                return {d: {"note": text} for d in dayobs_range(run_start, run_end)}

        adapter = UnicodeAdapter(fake_redis)
        result = adapter.fetch(20250101, 20250101)
        assert result[20250101]["note"] == text

        raw = fake_redis.get("adapter:recording:20250101")
        assert text.encode("utf-8") in raw
        assert b"\\u" not in raw

    def test_cached_null_is_a_hit(self, fake_redis):
        class NullAdapter(RecordingAdapter):
            def _fetch_run(self, run_start, run_end):
                self.calls.append((run_start, run_end))
                return {d: None for d in dayobs_range(run_start, run_end)}

        adapter = NullAdapter(fake_redis)
        adapter.fetch(20250101, 20250101)
        adapter.fetch(20250101, 20250101)
        assert len(adapter.calls) == 1

    def test_upstream_error_propagates_and_stores_nothing(self, fake_redis):
        adapter = RecordingAdapter(fake_redis, error=RuntimeError("upstream down"))
        with pytest.raises(RuntimeError):
            adapter.fetch(20250101, 20250103)
        assert fake_redis.keys() == []  # no data, and locks released

    def test_incomplete_source_result_raises_and_releases_locks(self, fake_redis):
        class IncompleteAdapter(RecordingAdapter):
            def _fetch_from_source(self, dayobs_list):
                return {}

        adapter = IncompleteAdapter(fake_redis)
        with pytest.raises(KeyError):
            adapter.fetch(20250101, 20250101)
        assert fake_redis.exists("lock:adapter:recording:20250101") == 0

    def test_non_finite_value_raises_and_releases_locks(self, fake_redis):
        class NanAdapter(RecordingAdapter):
            def _fetch_run(self, run_start, run_end):
                self.calls.append((run_start, run_end))
                return {d: float("nan") for d in dayobs_range(run_start, run_end)}

        adapter = NanAdapter(fake_redis)
        with pytest.raises(ValueError):
            adapter.fetch(20250101, 20250101)
        assert fake_redis.keys() == []  # no data, and locks released
        assert fake_redis.exists("lock:adapter:recording:20250101") == 0


class TestRunGrouping:
    """How missing dayobs are grouped into ``_fetch_run`` calls."""

    def test_one_run_is_fetched_inline(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        adapter.fetch(20250101, 20250105)
        assert adapter.calls == [(20250101, 20250105)]
        # No pool for the common case: one unbroken stretch of misses.
        assert adapter.call_threads == [threading.current_thread()]

    def test_a_cached_day_splits_the_misses_into_two_runs(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        prime(adapter, 20250102)
        adapter.fetch(20250101, 20250103)
        assert sorted(adapter.calls) == [(20250101, 20250101), (20250103, 20250103)]

    def test_days_outside_the_run_are_dropped(self, fake_redis):
        class OverreachingAdapter(RecordingAdapter):
            """Upstream that returns a day either side of the run.

            Real ones do this at range boundaries, where the upstream's
            end convention is exclusive or its rows straddle midnight.
            """

            def _fetch_run(self, run_start, run_end):
                super()._fetch_run(run_start, run_end)
                return {
                    dayobs: f"data-{dayobs}"
                    for dayobs in dayobs_range(
                        add_or_subtract_dayobs_days(run_start, -1),
                        add_or_subtract_dayobs_days(run_end, 1),
                    )
                }

        adapter = configured_adapter(fake_redis, adapter_class=OverreachingAdapter)
        assert adapter._fetch_from_source([20250102, 20250103]) == {
            20250102: "data-20250102",
            20250103: "data-20250103",
        }

    def test_a_day_outside_the_request_is_never_cached(self, fake_redis):
        class OverreachingAdapter(RecordingAdapter):
            def _fetch_run(self, run_start, run_end):
                super()._fetch_run(run_start, run_end)
                return {
                    dayobs: f"data-{dayobs}"
                    for dayobs in dayobs_range(run_start, add_or_subtract_dayobs_days(run_end, 1))
                }

        adapter = configured_adapter(fake_redis, adapter_class=OverreachingAdapter)
        adapter.fetch(20250101, 20250102)
        # Only keys this request holds a lock for may be written.
        assert sorted(fake_redis.keys()) == [
            "adapter:recording:20250101",
            "adapter:recording:20250102",
        ]

    def test_no_missing_days_fetches_nothing(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        assert adapter._fetch_from_source([]) == {}
        assert adapter.calls == []

    def test_refresh_fetches_its_one_day_inline(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        adapter.refresh(20250101)
        assert adapter.calls == [(20250101, 20250101)]
        assert adapter.call_threads == [threading.current_thread()]


class TestRunParallelism:
    """Runs beyond the first are fetched concurrently."""

    def two_run_adapter(self, fake_redis, **overrides):
        """An adapter whose next 20250101-05 request misses two runs."""
        adapter = configured_adapter(fake_redis, **overrides)
        prime(adapter, 20250103)
        return adapter

    def test_split_runs_overlap(self, fake_redis):
        adapter = self.two_run_adapter(fake_redis)
        # Each run waits for the other to arrive: a serial
        # implementation breaks the barrier and fails the fetch.
        barrier = threading.Barrier(2)
        adapter.on_fetch = lambda *_: barrier.wait(timeout=5)

        result = adapter.fetch(20250101, 20250105)

        assert sorted(adapter.calls) == [(20250101, 20250102), (20250104, 20250105)]
        assert result == {d: f"data-{d}" for d in dayobs_range(20250101, 20250105)}

    def test_split_runs_leave_the_calling_thread(self, fake_redis):
        adapter = self.two_run_adapter(fake_redis)
        # Held together so the pool cannot serve both from one reused
        # thread, which it may do when the first run finishes first.
        barrier = threading.Barrier(2)
        adapter.on_fetch = lambda *_: barrier.wait(timeout=5)

        adapter.fetch(20250101, 20250105)

        assert len(adapter.call_threads) == 2
        assert threading.current_thread() not in adapter.call_threads
        assert len(set(adapter.call_threads)) == 2

    def test_every_run_is_fetched_and_merged(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        prime(adapter, 20250102, 20250104, 20250106, 20250108)
        result = adapter.fetch(20250101, 20250109)
        assert sorted(adapter.calls) == [
            (20250101, 20250101),
            (20250103, 20250103),
            (20250105, 20250105),
            (20250107, 20250107),
            (20250109, 20250109),
        ]
        assert result == {d: f"data-{d}" for d in dayobs_range(20250101, 20250109)}

    def test_concurrency_is_capped(self, fake_redis):
        adapter = configured_adapter(fake_redis, MAX_PARALLEL_RUNS=2)
        prime(adapter, 20250102, 20250104, 20250106)
        # Runs are released in pairs, so the four runs make two whole
        # groups and none is left waiting for a partner the cap will
        # never let start.
        probe = ConcurrencyProbe(group_size=2)
        adapter.on_fetch = probe

        adapter.fetch(20250101, 20250107)

        # Four runs, two at a time: the second pair waits in the queue
        # rather than being dropped, raising, or joining the first.
        assert probe.max_active == 2
        assert len(adapter.calls) == 4

    def test_runs_over_the_cap_still_complete(self, fake_redis):
        adapter = configured_adapter(fake_redis, MAX_PARALLEL_RUNS=2)
        prime(adapter, 20250102, 20250104, 20250106, 20250108)
        result = adapter.fetch(20250101, 20250109)
        assert result == {d: f"data-{d}" for d in dayobs_range(20250101, 20250109)}
        assert len(adapter.call_threads) == 5

    @pytest.mark.parametrize("max_parallel", [1, 0])
    def test_cap_of_one_or_less_stays_sequential(self, fake_redis, max_parallel):
        # The escape hatch for an adapter whose client cannot be driven
        # from two threads: no pool, and runs in ascending order.
        adapter = configured_adapter(fake_redis, MAX_PARALLEL_RUNS=max_parallel)
        prime(adapter, 20250102, 20250104)
        result = adapter.fetch(20250101, 20250105)
        assert adapter.calls == [
            (20250101, 20250101),
            (20250103, 20250103),
            (20250105, 20250105),
        ]
        assert set(adapter.call_threads) == {threading.current_thread()}
        assert result == {d: f"data-{d}" for d in dayobs_range(20250101, 20250105)}

    def test_trace_id_reaches_the_worker_threads(self, fake_redis):
        adapter = self.two_run_adapter(fake_redis)
        seen = []
        adapter.on_fetch = lambda *_: seen.append(current_trace_id())
        previous = current_trace_id()
        set_trace_id("trace-abc")
        try:
            adapter.fetch(20250101, 20250105)
        finally:
            set_trace_id(previous)
        # A bare thread starts with an empty context, so without the
        # context copy these would log under no request at all.
        assert seen == ["trace-abc", "trace-abc"]

    def test_every_day_of_every_run_is_stored_and_unlocked(self, fake_redis):
        adapter = self.two_run_adapter(fake_redis)
        adapter.fetch(20250101, 20250105)
        # Storing happens back on the calling thread, after the runs
        # rejoin, so a fanned-out fetch caches exactly what an inline
        # one would.
        assert sorted(fake_redis.keys()) == [
            f"adapter:recording:{d}" for d in dayobs_range(20250101, 20250105)
        ]
        for dayobs in dayobs_range(20250101, 20250105):
            assert fake_redis.exists(f"lock:adapter:recording:{dayobs}") == 0


class TestRunFailures:
    """A failing run fails the whole collation."""

    def failing_run(self, target, error):
        """An ``on_fetch`` that raises only for one run."""

        def hook(run_start, run_end):
            if (run_start, run_end) == target:
                raise error

        return hook

    def test_one_failing_run_propagates(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        prime(adapter, 20250103)
        adapter.on_fetch = self.failing_run((20250104, 20250105), RuntimeError("upstream down"))
        with pytest.raises(RuntimeError, match="upstream down"):
            adapter.fetch(20250101, 20250105)

    def test_nothing_from_a_failed_collation_is_cached(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        prime(adapter, 20250103)
        adapter.on_fetch = self.failing_run((20250104, 20250105), RuntimeError("upstream down"))
        with pytest.raises(RuntimeError):
            adapter.fetch(20250101, 20250105)
        # The run that succeeded is discarded with the one that failed:
        # only the primed day survives, and every lock is released.
        assert fake_redis.keys() == ["adapter:recording:20250103"]
        for dayobs in dayobs_range(20250101, 20250105):
            assert fake_redis.exists(f"lock:adapter:recording:{dayobs}") == 0

    def test_the_earliest_failing_run_is_the_one_raised(self, fake_redis):
        adapter = configured_adapter(fake_redis)
        prime(adapter, 20250102)

        later_failed = threading.Event()

        def fail_late_then_early(run_start, run_end):
            if run_start == 20250103:
                later_failed.set()
                raise ValueError("later run")
            if run_start == 20250101:
                # Loses the race to raise, wins the report: the run
                # order decides, not whichever thread failed first.
                assert later_failed.wait(timeout=5)
                raise RuntimeError("earlier run")

        adapter.on_fetch = fail_late_then_early
        with pytest.raises(RuntimeError, match="earlier run"):
            adapter.fetch(20250101, 20250103)

    def test_queued_runs_are_dropped_after_a_failure(self, fake_redis, monkeypatch):
        cancelled = threading.Event()
        futures = []

        class WatchedExecutor(ThreadPoolExecutor):
            """Reports the moment a queued run is cancelled.

            `Future.cancel` fires the done callbacks itself, so this
            releases the held runs exactly when the cancellation has
            landed rather than after a sleep long enough to hope it
            has.
            """

            def submit(self, *args, **kwargs):
                future = super().submit(*args, **kwargs)
                future.add_done_callback(lambda done: cancelled.set() if done.cancelled() else None)
                futures.append(future)
                return future

        monkeypatch.setattr(base_adapters, "ThreadPoolExecutor", WatchedExecutor)

        adapter = configured_adapter(fake_redis, MAX_PARALLEL_RUNS=2)
        prime(adapter, 20250102, 20250104, 20250106, 20250108, 20250110)

        def fail_first_hold_the_rest(run_start, run_end):
            if run_start == 20250101:
                raise RuntimeError("upstream down")
            # Holding both workers until the cancellation lands is what
            # keeps the tail of the queue provably unstarted: nothing
            # can dequeue another run while they wait here.
            cancelled.wait(timeout=5)

        adapter.on_fetch = fail_first_hold_the_rest
        with pytest.raises(RuntimeError, match="upstream down"):
            adapter.fetch(20250101, 20250111)

        # Vacuous otherwise: the held runs would be released by the
        # timeout instead, and nothing would have been cancelled.
        assert cancelled.is_set()
        assert any(future.cancelled() for future in futures)
        # Six runs, two workers, one of them freed by the failure: at
        # most three can start, so the tail never reaches upstream.
        assert (20250101, 20250101) in adapter.calls
        assert len(adapter.calls) <= 3
        for queued in [(20250107, 20250107), (20250109, 20250109), (20250111, 20250111)]:
            assert queued not in adapter.calls

    def test_a_failure_in_the_inline_path_still_propagates(self, fake_redis):
        adapter = configured_adapter(fake_redis, MAX_PARALLEL_RUNS=1)
        prime(adapter, 20250102)
        adapter.on_fetch = self.failing_run((20250103, 20250103), RuntimeError("upstream down"))
        with pytest.raises(RuntimeError, match="upstream down"):
            adapter.fetch(20250101, 20250103)


class TestInstrumentRunSplitting:
    """The instrument base binds its instrument before the split."""

    def test_runs_split_per_instrument(self, fake_redis):
        adapter = configured_adapter(fake_redis, adapter_class=RecordingInstrumentAdapter)
        adapter.fetch("lsstcam", 20250102, 20250102)
        adapter.calls.clear()

        result = adapter.fetch("lsstcam", 20250101, 20250103)

        assert sorted(adapter.calls) == [
            ("lsstcam", 20250101, 20250101),
            ("lsstcam", 20250103, 20250103),
        ]
        assert result == {d: [{"day_obs": d}] for d in dayobs_range(20250101, 20250103)}

    def test_each_run_keeps_its_own_instrument(self, fake_redis):
        adapter = configured_adapter(fake_redis, adapter_class=RecordingInstrumentAdapter)
        adapter._fetch_from_source(["lsstcam:20250101", "latiss:20250101", "latiss:20250103"])

        # The instrument is bound per run, so a run fanned out to a
        # thread cannot pick up the loop's last instrument instead.
        assert sorted(adapter.calls) == [
            ("latiss", 20250101, 20250101),
            ("latiss", 20250103, 20250103),
            ("lsstcam", 20250101, 20250101),
        ]

    def test_runs_are_grouped_within_an_instrument_not_across(self, fake_redis):
        adapter = configured_adapter(fake_redis, adapter_class=RecordingInstrumentAdapter)
        # Adjacent days, but different instruments: two runs, not one.
        fetched = adapter._fetch_from_source(["latiss:20250101", "lsstcam:20250102"])
        assert sorted(adapter.calls) == [
            ("latiss", 20250101, 20250101),
            ("lsstcam", 20250102, 20250102),
        ]
        assert sorted(fetched) == ["latiss:20250101", "lsstcam:20250102"]


class TestTtlPolicy:
    def test_historical_gets_historic_ttl(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:recording:20200101"] == HISTORIC_TTL_REDIS

    def test_today_gets_today_ttl(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.fetch(TODAY, TODAY)
        assert fake_redis.ttls[f"adapter:recording:{TODAY}"] == TODAY_TTL_REDIS

    def test_mutable_mixin_shortens_historical_ttl(self, fake_redis):
        class MutableAdapter(MutableDataMixin, RecordingAdapter):
            pass

        adapter = MutableAdapter(fake_redis)
        adapter.fetch(20200101, 20200101)
        assert fake_redis.ttls["adapter:recording:20200101"] == MUTABLE_TTL_REDIS

    def test_mutable_mixin_keeps_today_ttl(self, fake_redis):
        class MutableAdapter(MutableDataMixin, RecordingAdapter):
            pass

        adapter = MutableAdapter(fake_redis)
        adapter.fetch(TODAY, TODAY)
        assert fake_redis.ttls[f"adapter:recording:{TODAY}"] == TODAY_TTL_REDIS

    def test_instrument_keys_get_today_ttl(self, fake_redis):
        adapter = RecordingInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", TODAY, TODAY)
        assert fake_redis.ttls[f"adapter:recording_instrument:lsstcam:{TODAY}"] == TODAY_TTL_REDIS

    def test_instrument_keys_get_historic_ttl(self, fake_redis):
        adapter = RecordingInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", 20200101, 20200101)
        assert fake_redis.ttls["adapter:recording_instrument:lsstcam:20200101"] == HISTORIC_TTL_REDIS

    def test_mutable_mixin_keeps_today_ttl_for_instrument_keys(self, fake_redis):
        class MutableInstrumentAdapter(MutableDataMixin, RecordingInstrumentAdapter):
            pass

        adapter = MutableInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", TODAY, TODAY)
        assert fake_redis.ttls[f"adapter:recording_instrument:lsstcam:{TODAY}"] == TODAY_TTL_REDIS

    def test_mutable_mixin_shortens_historical_instrument_ttl(self, fake_redis):
        class MutableInstrumentAdapter(MutableDataMixin, RecordingInstrumentAdapter):
            pass

        adapter = MutableInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", 20200101, 20200101)
        assert fake_redis.ttls["adapter:recording_instrument:lsstcam:20200101"] == MUTABLE_TTL_REDIS


class TestIsToday:
    """The hook every TTL policy reads a key through."""

    def test_dayobs_key(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        assert adapter._is_today(TODAY) is True
        assert adapter._is_today(20200101) is False

    def test_composite_instrument_key(self, fake_redis):
        adapter = RecordingInstrumentAdapter(fake_redis)
        assert adapter._is_today(f"lsstcam:{TODAY}") is True
        assert adapter._is_today("lsstcam:20200101") is False

    def test_id_key_is_never_today(self, fake_redis):
        adapter = RecordingIdAdapter(fake_redis)
        assert adapter._is_today("BLOCK-1") is False
        # An ID that happens to look like today's dayobs is still an ID.
        assert adapter._is_today(str(TODAY)) is False

    def test_key_shape_must_answer(self, fake_redis):
        class NoKeyShapeAdapter(CachedAdapter):
            name = "no_key_shape"

        with pytest.raises(NotImplementedError):
            NoKeyShapeAdapter(fake_redis)._ttl("any-key")


class TestRefresh:
    def test_refresh_overwrites_in_place(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        fake_redis.set("adapter:recording:20250101", '"stale"')
        adapter.refresh(20250101)
        assert adapter.fetch(20250101, 20250101) == {20250101: "data-20250101"}

    def test_failed_refresh_leaves_old_entry(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        fake_redis.set("adapter:recording:20250101", '"previous"')
        adapter.error = RuntimeError("upstream down")
        with pytest.raises(RuntimeError):
            adapter.refresh(20250101)
        assert adapter.fetch(20250101, 20250101) == {20250101: "previous"}


class TestSingleFlight:
    def test_concurrent_misses_fetch_once(self, fake_redis):
        adapter = RecordingAdapter(fake_redis, delay=0.05)
        results = {}

        def request(slot):
            results[slot] = adapter.fetch(20250101, 20250101)

        threads = [threading.Thread(target=request, args=(i,)) for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len(adapter.calls) == 1
        assert all(r == {20250101: "data-20250101"} for r in results.values())

    def test_waiter_takes_over_when_lock_expires_without_entry(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        # Simulate a fetch holder that died without storing or
        # releasing: the lock exists but will expire shortly.
        fake_redis.set("lock:adapter:recording:20250101", "1", ex=0.05)
        result = adapter.fetch(20250101, 20250101)
        assert result == {20250101: "data-20250101"}
        assert adapter.calls == [(20250101, 20250101)]

    def test_lock_released_after_successful_fetch(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.fetch(20250101, 20250101)
        assert fake_redis.exists("lock:adapter:recording:20250101") == 0

    def test_lock_winner_rechecks_cache_before_fetching(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        real_acquire = adapter._acquire_lock

        def acquire_after_other_request_stored(key):
            # Another request stores the entry and releases its lock
            # in the window between our cache check and lock win.
            fake_redis.set(adapter._cache_key(key), '"other-result"', ex=60)
            return real_acquire(key)

        adapter._acquire_lock = acquire_after_other_request_stored
        result = adapter.fetch(20250101, 20250101)
        assert result == {20250101: "other-result"}
        assert adapter.calls == []
        assert fake_redis.exists("lock:adapter:recording:20250101") == 0


class TestPartialLockCollision:
    """One request winning some locks and losing others in the same pass.

    Request A is fetching 20250101-03; request B arrives asking for
    20250103-05. B wins 04 and 05, loses the shared 03, and has to
    make progress on what it won without re-fetching or interfering
    with what A holds.

    A is simulated rather than run: its locks are planted in the cache
    and its entries appear at the moment the code under test would
    have collided with it, which keeps these deterministic.
    """

    CONTENDED = 20250103

    def hold_lock(self, adapter, fake_redis, dayobs, ex=60):
        """Request A's lock, with no entry stored behind it yet."""
        fake_redis.set(adapter._lock_key(dayobs), "1", ex=ex)

    def store_as_request_a(self, adapter, fake_redis, dayobs, release=True):
        """Request A completing: its entry stored, its lock dropped."""
        fake_redis.set(adapter._cache_key(dayobs), f'"from-request-a-{dayobs}"', ex=60)
        if release:
            fake_redis.delete(adapter._lock_key(dayobs))

    def test_the_contended_key_is_left_out_of_the_fetch(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        # A finishes while B is upstream for the keys it won.
        adapter.on_fetch = lambda *_: self.store_as_request_a(adapter, fake_redis, self.CONTENDED)
        # Short-lived so a fetch ordered after the wait cannot hang.
        self.hold_lock(adapter, fake_redis, self.CONTENDED, ex=1.0)

        result = adapter.fetch(20250103, 20250105)

        # 03-05 is one contiguous run, but runs are collated from the
        # keys B won, so the shared day is not fetched a second time.
        assert adapter.calls == [(20250104, 20250105)]
        assert result == {
            20250103: "from-request-a-20250103",
            20250104: "data-20250104",
            20250105: "data-20250105",
        }

    def test_the_other_requests_lock_is_left_alone(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        # A stores its entry but has not released its lock yet.
        adapter.on_fetch = lambda *_: self.store_as_request_a(
            adapter, fake_redis, self.CONTENDED, release=False
        )
        self.hold_lock(adapter, fake_redis, self.CONTENDED)

        adapter.fetch(20250103, 20250105)

        # Releasing a lock B never won would hand A's key to a third
        # request mid-fetch.
        assert fake_redis.exists(adapter._lock_key(self.CONTENDED)) == 1
        assert fake_redis.exists(adapter._lock_key(20250104)) == 0
        assert fake_redis.exists(adapter._lock_key(20250105)) == 0

    def test_own_locks_are_released_before_waiting(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.on_fetch = lambda *_: self.store_as_request_a(adapter, fake_redis, self.CONTENDED)
        self.hold_lock(adapter, fake_redis, self.CONTENDED)
        still_held = []
        real_wait = adapter._wait_for_entry

        def recording_wait(key):
            still_held.extend(
                day for day in (20250104, 20250105) if fake_redis.exists(adapter._lock_key(day))
            )
            return real_wait(key)

        adapter._wait_for_entry = recording_wait
        adapter.fetch(20250103, 20250105)

        # Blocking on another request's key while holding locks of
        # your own is what lets two crossed requests deadlock.
        assert still_held == []

    def test_orphaned_contended_lock_is_taken_over_after_the_partial_fetch(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        # A died mid-fetch: lock about to expire, nothing stored.
        self.hold_lock(adapter, fake_redis, self.CONTENDED, ex=0.05)

        result = adapter.fetch(20250103, 20250105)

        # The keys B won first, then a second pass that picks up the
        # abandoned one.
        assert adapter.calls == [(20250104, 20250105), (20250103, 20250103)]
        assert result == {d: f"data-{d}" for d in dayobs_range(20250103, 20250105)}

    def test_fully_contended_request_fetches_nothing(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        for dayobs in dayobs_range(20250103, 20250105):
            self.hold_lock(adapter, fake_redis, dayobs)
        real_wait = adapter._wait_for_entry

        def store_then_wait(key):
            # A's entries land while B polls; with no key won there is
            # no fetch of B's own to overlap with.
            self.store_as_request_a(adapter, fake_redis, key)
            return real_wait(key)

        adapter._wait_for_entry = store_then_wait

        result = adapter.fetch(20250103, 20250105)

        assert adapter.calls == []
        assert result == {d: f"from-request-a-{d}" for d in dayobs_range(20250103, 20250105)}

    def test_a_won_key_cached_during_the_race_splits_the_fetch_run(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        self.hold_lock(adapter, fake_redis, self.CONTENDED)
        adapter.on_fetch = lambda *_: self.store_as_request_a(adapter, fake_redis, self.CONTENDED)
        real_acquire = adapter._acquire_lock

        def acquire_racing_with_another_store(key):
            won = real_acquire(key)
            if key == 20250104:
                # Stored between B's cache check and its lock win, so
                # the double-check drops it from the fetch.
                self.store_as_request_a(adapter, fake_redis, key, release=False)
            return won

        adapter._acquire_lock = acquire_racing_with_another_store

        result = adapter.fetch(20250103, 20250105)

        assert adapter.calls == [(20250105, 20250105)]
        assert result[20250104] == "from-request-a-20250104"
        assert fake_redis.exists(adapter._lock_key(20250104)) == 0


class TestConcurrentPartialOverlap:
    """The same collision run for real, with two threads."""

    def test_overlapping_requests_fetch_each_key_once(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        a_is_upstream = threading.Event()
        a_may_finish = threading.Event()

        def hold_the_shared_key(run_start, run_end):
            if run_start == 20250101:
                a_is_upstream.set()
                a_may_finish.wait(timeout=5)

        adapter.on_fetch = hold_the_shared_key
        results = {}

        def request(slot, start, end):
            results[slot] = adapter.fetch(start, end)

        request_a = threading.Thread(target=request, args=("a", 20250101, 20250103))
        request_a.start()
        assert a_is_upstream.wait(timeout=5)
        request_b = threading.Thread(target=request, args=("b", 20250103, 20250105))
        request_b.start()

        # B gets through the keys it won while A is still upstream,
        # rather than serialising behind A's in-flight key.
        deadline = time.monotonic() + 5
        while (20250104, 20250105) not in adapter.calls and time.monotonic() < deadline:
            time.sleep(0.005)
        assert (20250104, 20250105) in adapter.calls
        a_may_finish.set()
        request_a.join(timeout=5)
        request_b.join(timeout=5)

        assert not request_a.is_alive() and not request_b.is_alive()
        # The shared key was fetched by A only.
        assert sorted(adapter.calls) == [(20250101, 20250103), (20250104, 20250105)]
        assert results["a"] == {d: f"data-{d}" for d in dayobs_range(20250101, 20250103)}
        assert results["b"] == {d: f"data-{d}" for d in dayobs_range(20250103, 20250105)}

    def test_crossed_key_order_does_not_deadlock(self, fake_redis):
        # Only ID keys can arrive in arbitrary order — dayobs ranges
        # are always ascending — so two requests can want the same
        # keys in opposite orders.
        adapter = RecordingIdAdapter(fake_redis, delay=0.05)
        ids = ["BLOCK-1", "BLOCK-2", "BLOCK-3"]
        results = {}

        def request(slot, requested):
            results[slot] = adapter.fetch_by_ids(requested)

        threads = [
            threading.Thread(target=request, args=("a", ids)),
            threading.Thread(target=request, args=("b", list(reversed(ids)))),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        assert not any(thread.is_alive() for thread in threads)
        expected = {id_: f"detail-{id_}" for id_ in ids}
        assert results["a"] == expected
        assert results["b"] == expected
        # However the locks were split, each ID was fetched once.
        assert sorted(id_ for call in adapter.calls for id_ in call) == ids


class TestIdCachedAdapter:
    def test_cold_and_hot_by_id(self, fake_redis):
        adapter = RecordingIdAdapter(fake_redis)
        result = adapter.fetch_by_ids(["BLOCK-1", "BLOCK-T2"])
        assert result == {"BLOCK-1": "detail-BLOCK-1", "BLOCK-T2": "detail-BLOCK-T2"}
        adapter.calls.clear()
        adapter.fetch_by_ids(["BLOCK-1", "BLOCK-T2"])
        assert adapter.calls == []

    def test_partial_fetches_only_missing_ids(self, fake_redis):
        adapter = RecordingIdAdapter(fake_redis)
        adapter.fetch_by_ids(["BLOCK-1"])
        adapter.calls.clear()
        adapter.fetch_by_ids(["BLOCK-1", "BLOCK-2", "BLOCK-3"])
        assert adapter.calls == [["BLOCK-2", "BLOCK-3"]]

    def test_fixed_historic_ttl_and_key_scheme(self, fake_redis):
        adapter = RecordingIdAdapter(fake_redis)
        adapter.fetch_by_ids(["BLOCK-1"])
        assert fake_redis.ttls["adapter:recording_ids:BLOCK-1"] == HISTORIC_TTL_REDIS

    def test_mutable_mixin_applies_to_id_keys(self, fake_redis):
        class MutableIdAdapter(MutableDataMixin, RecordingIdAdapter):
            pass

        adapter = MutableIdAdapter(fake_redis)
        adapter.fetch_by_ids(["BLOCK-1"])
        assert fake_redis.ttls["adapter:recording_ids:BLOCK-1"] == MUTABLE_TTL_REDIS
