#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Adapter base classes for the Redis-backed caching architecture."""

import json
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from lsst.ts.logging_and_reporting.cache_ttl import (
    HISTORIC_TTL_REDIS,
    TODAY_TTL_REDIS,
)
from lsst.ts.logging_and_reporting.utils.dayobs import (
    contiguous_runs,
    current_dayobs,
    dayobs_range,
)

logger = logging.getLogger(__name__)


class CachedAdapter:
    """Base for all cached adapters: a Redis cache loop with per-key
    single-flight locks.

    Shared by `DayobsCachedAdapter` (integer dayobs keys),
    `IdCachedAdapter` (string ID keys), and
    `InstrumentDayobsCachedAdapter` (instrument+dayobs keys). Subclasses
    must set ``name``, implement ``_is_today`` and ``_fetch_from_source``, and
    add the public accessor for their key shape
    (``fetch``/``fetch_by_ids``).

    """

    name: str
    """Adapter identifier; namespaces this adapter's cache keys."""

    LOCK_TTL = 30
    """Single-flight lock expiry (seconds).

    Must exceed the slowest expected upstream fetch; it exists only so
    a crashed lock holder cannot block a key forever.
    """

    POLL_INTERVAL = 0.1
    """Sleep (seconds) between cache polls while another request
    holds the fetch lock for a key we need."""

    def __init__(self, redis: Any):
        self._redis = redis

    def _is_today(self, key) -> bool:
        """Whether ``key`` covers the current dayobs.

        The single point at which TTL policy interrogates a key; each
        key-shape subclass answers for its own key form.
        """
        raise NotImplementedError

    def _ttl(self, key) -> int:
        """Today's entry gets the short TTL, everything else the
        historic one. Adapters with different lifetimes override this
        (or mix in `MutableDataMixin`)."""
        return TODAY_TTL_REDIS if self._is_today(key) else HISTORIC_TTL_REDIS

    def _fetch_from_source(self, keys: list) -> dict:
        """Fetch a set of keys from the upstream source"""
        raise NotImplementedError

    def _empty_value(self) -> Any:
        """Seed for a key with no data (default: an empty row list)."""
        return []

    @staticmethod
    def _partition_by_field(
        rows: list, key: str | Callable = "day_obs"
    ) -> dict[Any, list]:
        """Bucket rows by a field value or a computed key.

        Parameters
        ----------
        rows : `list`
            Flat list of rows (typically dicts).
        key : `str` or `Callable`, optional
            Either a field name (looked up via ``row.get``) or a callable
            that takes a row and returns the partition key. A row that
            yields ``None`` from a string field lands under ``None``.

        Returns
        -------
        `dict` [`Any`, `list`]
            Rows grouped by key; partition values are the original rows.
        """
        partition: dict[Any, list] = defaultdict(list)
        for row in rows:
            k = key(row) if callable(key) else row.get(key)
            partition[k].append(row)
        return partition

    def _collate_runs(
        self, dayobs_list: list[int], fetch_run: Callable[[int, int], dict[int, Any]]
    ) -> dict[int, Any]:
        """Seed each dayobs, then fill from one ``fetch_run`` per run.

        Issues one fetch per contiguous run and merges each run's dayobs
        partition back in.
        """
        results: dict[int, Any] = {
            dayobs: self._empty_value() for dayobs in dayobs_list
        }
        for run_start, run_end in contiguous_runs(dayobs_list):
            logger.debug(f"Fetching {self.name} for dayobs {run_start}..{run_end}")
            for dayobs, value in fetch_run(run_start, run_end).items():
                if dayobs in results:
                    results[dayobs] = value
        return results

    def _cache_key(self, key) -> str:
        """The Redis key for one entry, namespaced by adapter name.

        e.g. ``"adapter:exposure_entries:20250101"`` or
        ``"adapter:block_detail:BLOCK-42"``.
        """
        return f"adapter:{self.name}:{key}"

    def _lock_key(self, key) -> str:
        """The single-flight lock key paired with ``_cache_key``.

        Its existence signals that a fetch for the entry is in flight.
        """
        return f"lock:{self._cache_key(key)}"

    def _check_cache(self, key) -> tuple[bool, Any]:
        """Look up one key; return ``(hit, value)``.

        The flag distinguishes a miss from a cached JSON ``null``.
        """
        raw = self._redis.get(self._cache_key(key))
        if raw is None:
            return False, None
        return True, json.loads(raw)

    def _store(self, key, data: Any) -> None:
        """Serialise ``data`` as JSON and write it with the key's TTL.

        ``_fetch_from_source`` implementations must therefore return
        JSON-serialisable data. ``allow_nan=False`` rejects NaN/Infinity
        instead of writing spec-invalid JSON; ``ensure_ascii=False`` and
        compact separators keep the cached bytes smaller.
        """
        self._redis.set(
            self._cache_key(key),
            json.dumps(
                data, allow_nan=False, ensure_ascii=False, separators=(",", ":")
            ),
            ex=self._ttl(key),
        )

    def _acquire_lock(self, key) -> bool:
        return bool(
            self._redis.set(self._lock_key(key), "1", nx=True, ex=self.LOCK_TTL)
        )

    def _release_lock(self, key) -> None:
        # Unconditional DEL: if our lock expired mid-fetch and another
        # request re-acquired it, deleting theirs costs at worst one
        # redundant upstream fetch.
        self._redis.delete(self._lock_key(key))

    def _fetch_cached(self, keys: list) -> dict:
        """The cache loop: serve hits, single-flight-fetch the misses.

        If every key is cached the upstream source is never contacted.
        For misses, a per-key Redis lock (``SET NX EX``) partitions the
        work: this request batch-fetches the keys whose locks it won,
        while keys whose locks are held elsewhere are polled until the
        entry appears. Any upstream error propagates immediately and
        the entire request fails — partial data is never returned.
        """
        results = {}
        missing = []
        for key in keys:
            hit, value = self._check_cache(key)
            if hit:
                results[key] = value
            else:
                missing.append(key)
        if not missing:
            return results

        pending = missing
        while pending:
            won = [key for key in pending if self._acquire_lock(key)]
            lost = [key for key in pending if key not in won]
            # Double-check won keys: another request may have stored
            # the entry (and released its lock) between our cache
            # check and winning the lock.
            to_fetch = []
            for key in won:
                hit, value = self._check_cache(key)
                if hit:
                    results[key] = value
                    self._release_lock(key)
                else:
                    to_fetch.append(key)
            if to_fetch:
                try:
                    fetched = self._fetch_from_source(to_fetch)
                    for key in to_fetch:
                        if key not in fetched:
                            raise KeyError(
                                f"{type(self).__name__}._fetch_from_source did not "
                                f"return an entry for {key!r}; it must cover every "
                                "requested key (use an empty container for no data)"
                            )
                        self._store(key, fetched[key])
                        results[key] = fetched[key]
                finally:
                    for key in to_fetch:
                        self._release_lock(key)
            pending = []
            for key in lost:
                hit, value = self._wait_for_entry(key)
                if hit:
                    results[key] = value
                else:
                    # The lock vanished with no entry appearing: the
                    # holder's fetch failed or the holder died. Retry
                    # lock acquisition so the fetch (and any upstream
                    # error) happens in this request instead.
                    pending.append(key)
        return results

    def _wait_for_entry(self, key) -> tuple[bool, Any]:
        """Poll the cache while another request fetches ``key``.

        Returns ``(True, value)`` once the entry appears, or
        ``(False, None)`` if the fetch lock disappeared without an
        entry being stored.
        """
        while True:
            hit, value = self._check_cache(key)
            if hit:
                return True, value
            if not self._redis.exists(self._lock_key(key)):
                return False, None
            time.sleep(self.POLL_INTERVAL)


class DayobsCachedAdapter(CachedAdapter, ABC):
    """Base class for all dayobs-driven adapters.

    Implements `fetch` as a Redis cache lookup over the dayobs range;
    subclasses set ``name`` and implement `_fetch_run`.
    """

    name: str = "dayobs"

    def fetch(self, start_dayobs: int, end_dayobs: int) -> dict[int, Any]:
        """Return data for the dayobs range, partitioned by dayobs.

        Parameters
        ----------
        start_dayobs : `int`
            First dayobs of the range, in YYYYMMDD form.
        end_dayobs : `int`
            Last dayobs of the range (inclusive), in YYYYMMDD form.

        Returns
        -------
        `dict` [`int`, `Any`]
            One entry per dayobs in the range.
        """
        return self._fetch_cached(dayobs_range(start_dayobs, end_dayobs))

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, Any]:
        """Collate the cache-missing dayobs, one `_fetch_run` per run."""
        return self._collate_runs(dayobs_list, self._fetch_run)

    @abstractmethod
    def _fetch_run(self, run_start: int, run_end: int) -> dict[int, Any]:
        """Fetch one contiguous dayobs run, partitioned by dayobs. Implemented by
        the concrete subclass.

        End with `_partition_by_field` to bucket the flat rows by dayobs.

        Parameters
        ----------
        run_start, run_end : `int`
            Inclusive bounds of the run, in YYYYMMDD form.

        Returns
        -------
        `dict` [`int`, `Any`]
            The run's JSON-serialisable data keyed by dayobs.
        """
        raise NotImplementedError

    def _is_today(self, dayobs: int) -> bool:
        """Whether a key (dayobs) is the current dayobs"""
        return dayobs == current_dayobs()

    def refresh(self, dayobs: int) -> None:
        """Refresh one dayobs' cache entry, used by ``RefreshWorker``
        to keep todays cache fresh.

        Fetches fresh data first and then replaces the old value with
        a single Redis ``SET`` — the existing entry is never deleted
        ahead of the fetch, so requests arriving mid-refresh are served
        the previous value instead of falling into a cold-miss window.
        If the fetch fails the old entry is left untouched.

        Bypasses the single-flight lock: by construction, this will
        always be a key which already exists."""
        data = self._fetch_from_source([dayobs])
        self._store(dayobs, data[dayobs])


class InstrumentDayobsCachedAdapter(CachedAdapter, ABC):
    """Base class for adapters partitioned by instrument and dayobs.

    Cache keys are the composite ``"{instrument}:{dayobs}"`` — used when
    an upstream is per-instrument (so all instruments cannot share one
    dayobs key) but still dayobs-driven. Owns the cache mechanics only:
    subclasses provide ``_fetch_run(instrument, run_start, run_end)``
    returning the run's rows partitioned by dayobs (via
    `_partition_by_field`), and may add request validation by overriding
    `fetch`.
    """

    INSTRUMENTS = ("lsstcam", "latiss")
    """Instruments this app serves: the RefreshWorker warms each for
    today, and subclasses validate requests against this set.
    """

    def fetch(
        self, instrument: str, start_dayobs: int, end_dayobs: int
    ) -> dict[int, list[dict]]:
        """Return rows for the range, partitioned by dayobs.

        Parameters
        ----------
        instrument : `str`
            Instrument name; lower-cased for the cache key.
        start_dayobs, end_dayobs : `int`
            Inclusive bounds of the range, in YYYYMMDD form.

        Returns
        -------
        `dict` [`int`, `Any`]
            One entry per dayobs in the range.
        """
        instrument = instrument.lower()
        days = dayobs_range(start_dayobs, end_dayobs)
        by_key = self._fetch_cached(
            [self._compose_key(instrument, day) for day in days]
        )
        return {day: by_key[self._compose_key(instrument, day)] for day in days}

    def refresh(self, dayobs: int) -> None:
        """See ``DayobsCachedAdapter.refresh``

        Extend to iterate over `INSTRUMENTS`, so the
        served entry is never emptied. Instruments are independent: one
        instrument's upstream failure is logged and the rest still run.
        """
        for instrument in self.INSTRUMENTS:
            key = self._compose_key(instrument, dayobs)
            try:
                fetched = self._fetch_from_source([key])
                self._store(key, fetched[key])
            except Exception:
                logger.exception(
                    f"{self.name} refresh failed for {instrument} dayobs {dayobs}"
                )

    @staticmethod
    def _compose_key(instrument: str, dayobs: int) -> str:
        """Construct a composite key"""
        return f"{instrument}:{dayobs}"

    @staticmethod
    def _split_key(key: str) -> tuple[str, int]:
        """Turn a composite key into a tuple of its values"""
        instrument, dayobs = key.rsplit(":", 1)
        return instrument, int(dayobs)

    def _is_today(self, key: str) -> bool:
        return self._split_key(key)[1] == current_dayobs()

    def _fetch_from_source(self, keys: list[str]) -> dict[str, list[dict]]:
        # Composite keys may span instruments, each with its own schema:
        # collate one instrument's runs at a time (sharing the dayobs loop
        # and out-of-range guard), then re-key the per-dayobs partition to
        # the composite cache key.
        by_instrument: dict[str, list[int]] = defaultdict(list)
        for key in keys:
            instrument, dayobs = self._split_key(key)
            by_instrument[instrument].append(dayobs)
        results: dict[str, list[dict]] = {}
        for instrument, dayobs_list in by_instrument.items():
            per_day = self._collate_runs(
                dayobs_list,
                lambda run_start, run_end, i=instrument: self._fetch_run(
                    i, run_start, run_end
                ),
            )
            for dayobs, rows in per_day.items():
                results[self._compose_key(instrument, dayobs)] = rows
        return results

    @abstractmethod
    def _fetch_run(
        self, instrument: str, run_start: int, run_end: int
    ) -> dict[int, list[dict]]:
        """Fetch one contiguous dayobs+instrument run, partitioned by dayobs. Implemented by
        the concrete subclass.

        End with `_partition_by_field` to bucket the flat rows by dayobs.

        Parameters
        ----------
        instrument : `str`
            Instrument name; lower-cased for the cache key.
        run_start, run_end : `int`
            Inclusive bounds of the run, in YYYYMMDD form.

        Returns
        -------
        `dict` [`int`, `Any`]
            The run's JSON-serialisable data keyed by dayobs.
        """
        raise NotImplementedError


class IdCachedAdapter(CachedAdapter, ABC):
    """Base class for adapters keyed by opaque IDs rather than dayobs.

    Runs the same cache loop as `DayobsCachedAdapter` (including single-flight
    locks), adapted for string keys, with a TTL no longer based off dayobs.
    Subclasses set ``name`` and implement `_fetch_from_source`.
    """

    name: str = "id_based"

    def fetch_by_ids(self, ids: list[str]) -> dict[str, Any]:
        """Return records for the given IDs, keyed by ID.

        Cached IDs are served from Redis; only the misses are fetched
        upstream.

        Parameters
        ----------
        ids : `list` [`str`]
            The IDs to look up, in whatever form this adapter's cache
            keys take (e.g. ``BLOCK-42``).

        Returns
        -------
        `dict` [`str`, `Any`]
            One entry per requested ID. A stored ``None`` is a value
            meaning "no such record upstream" rather than a miss.
        """
        return self._fetch_cached(ids)

    @abstractmethod
    def _fetch_from_source(self, ids: list[str]) -> dict[str, Any]:
        """Fetch the given (cache-missing) IDs from upstream. Implemented
        by the concrete subclass.

        Parameters
        ----------
        ids : `list` [`str`]
            The IDs the cache loop could not serve, passed as one
            batch so the implementation can collapse them into a
            single upstream query if possible.

        Returns
        -------
        `dict` [`str`, `Any`]
            JSON-serialisable data with an entry for every requested
            ID. An ID with no data must map to an explicit value
            (``None``, or an empty container) rather than be omitted;
            the cache loop raises `KeyError` naming the adapter
            otherwise, since an absent key is indistinguishable from a
            cache miss and would be re-fetched forever.
        """
        raise NotImplementedError

    def _is_today(self, id_: str) -> bool:
        """Always False — ID-keyed records have no today/historic split."""
        return False
