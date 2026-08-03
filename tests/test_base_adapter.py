import threading
import time

import pytest

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
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs, dayobs_range

TODAY = current_dayobs()


class RecordingAdapter(DayobsCachedAdapter):
    """DayobsCachedAdapter whose upstream returns f"data-{dayobs}" per day."""

    name = "recording"
    POLL_INTERVAL = 0.005

    def __init__(self, redis, delay=0.0, error=None, on_fetch=None):
        super().__init__(redis)
        self.calls = []
        self.delay = delay
        self.error = error
        # Called while this request is "upstream", i.e. holding the
        # locks it won: the window another request collides with.
        self.on_fetch = on_fetch

    def _fetch_run(self, run_start, run_end):
        self.calls.append((run_start, run_end))
        if self.on_fetch is not None:
            self.on_fetch(run_start, run_end)
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return {dayobs: f"data-{dayobs}" for dayobs in dayobs_range(run_start, run_end)}


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
        return {
            dayobs: [{"day_obs": dayobs}] for dayobs in dayobs_range(run_start, run_end)
        }


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
        # The cached 20250102 splits the misses into two single-day runs.
        assert adapter.calls == [(20250101, 20250101), (20250103, 20250103)]

    def test_values_json_roundtrip(self, fake_redis):
        class StructuredAdapter(RecordingAdapter):
            def _fetch_run(self, run_start, run_end):
                self.calls.append((run_start, run_end))
                return {
                    d: {"n": d, "items": [1, 2], "empty": None}
                    for d in dayobs_range(run_start, run_end)
                }

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
        assert (
            fake_redis.ttls[f"adapter:recording_instrument:lsstcam:{TODAY}"]
            == TODAY_TTL_REDIS
        )

    def test_instrument_keys_get_historic_ttl(self, fake_redis):
        adapter = RecordingInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", 20200101, 20200101)
        assert (
            fake_redis.ttls["adapter:recording_instrument:lsstcam:20200101"]
            == HISTORIC_TTL_REDIS
        )

    def test_mutable_mixin_keeps_today_ttl_for_instrument_keys(self, fake_redis):
        class MutableInstrumentAdapter(MutableDataMixin, RecordingInstrumentAdapter):
            pass

        adapter = MutableInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", TODAY, TODAY)
        assert (
            fake_redis.ttls[f"adapter:recording_instrument:lsstcam:{TODAY}"]
            == TODAY_TTL_REDIS
        )

    def test_mutable_mixin_shortens_historical_instrument_ttl(self, fake_redis):
        class MutableInstrumentAdapter(MutableDataMixin, RecordingInstrumentAdapter):
            pass

        adapter = MutableInstrumentAdapter(fake_redis)
        adapter.fetch("lsstcam", 20200101, 20200101)
        assert (
            fake_redis.ttls["adapter:recording_instrument:lsstcam:20200101"]
            == MUTABLE_TTL_REDIS
        )


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
        adapter.on_fetch = lambda *_: self.store_as_request_a(
            adapter, fake_redis, self.CONTENDED
        )
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
        adapter.on_fetch = lambda *_: self.store_as_request_a(
            adapter, fake_redis, self.CONTENDED
        )
        self.hold_lock(adapter, fake_redis, self.CONTENDED)
        still_held = []
        real_wait = adapter._wait_for_entry

        def recording_wait(key):
            still_held.extend(
                day
                for day in (20250104, 20250105)
                if fake_redis.exists(adapter._lock_key(day))
            )
            return real_wait(key)

        adapter._wait_for_entry = recording_wait
        adapter.fetch(20250103, 20250105)

        # Blocking on another request's key while holding locks of
        # your own is what lets two crossed requests deadlock.
        assert still_held == []

    def test_orphaned_contended_lock_is_taken_over_after_the_partial_fetch(
        self, fake_redis
    ):
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
        assert result == {
            d: f"from-request-a-{d}" for d in dayobs_range(20250103, 20250105)
        }

    def test_a_won_key_cached_during_the_race_splits_the_fetch_run(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        self.hold_lock(adapter, fake_redis, self.CONTENDED)
        adapter.on_fetch = lambda *_: self.store_as_request_a(
            adapter, fake_redis, self.CONTENDED
        )
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
        assert results["a"] == {
            d: f"data-{d}" for d in dayobs_range(20250101, 20250103)
        }
        assert results["b"] == {
            d: f"data-{d}" for d in dayobs_range(20250103, 20250105)
        }

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
