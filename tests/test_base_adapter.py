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

    def __init__(self, redis, delay=0.0, error=None):
        super().__init__(redis)
        self.calls = []
        self.delay = delay
        self.error = error

    def _fetch_run(self, run_start, run_end):
        self.calls.append((run_start, run_end))
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return {dayobs: f"data-{dayobs}" for dayobs in dayobs_range(run_start, run_end)}


class RecordingIdAdapter(IdCachedAdapter):
    name = "recording_ids"
    POLL_INTERVAL = 0.005

    def __init__(self, redis):
        super().__init__(redis)
        self.calls = []

    def _fetch_from_source(self, ids):
        self.calls.append(list(ids))
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
        # The cached 20250102 splits the misses into two single-day runs.
        assert adapter.calls == [(20250101, 20250101), (20250103, 20250103)]

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

    def test_refresh_today_targets_current_dayobs(self, fake_redis):
        adapter = RecordingAdapter(fake_redis)
        adapter.refresh_today()
        assert fake_redis.get(f"adapter:recording:{TODAY}") is not None


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
