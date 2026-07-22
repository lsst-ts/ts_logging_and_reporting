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

"""Adapter base classes for the Redis-backed caching architecture.

Adapters own
data fetching and caching; the service layer only collates. The cache
loop (including single-flight stampede protection) lives here, and
cache keys are derived from each adapter's ``name``, so concrete
adapters implement only ``_fetch_from_source`` and set ``name``.
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any

from lsst.ts.logging_and_reporting.cache_ttl import (
    HISTORIC_TTL_REDIS,
    TODAY_TTL_REDIS,
)
from lsst.ts.logging_and_reporting.utils.dayobs import contiguous_runs, current_dayobs, dayobs_range

logger = logging.getLogger(__name__)


class CachedAdapter:
    """Base for all cached adapters: a Redis cache loop with per-key
    single-flight locks.

    Shared by `DayobsCachedAdapter` (integer dayobs keys),
    `IdCachedAdapter` (string ID keys), and
    `InstrumentDayobsCachedAdapter` (instrument+dayobs keys). Subclasses
    set ``name``, provide ``_ttl`` and ``_fetch_from_source``, and add
    the public accessor for their key shape (``fetch``/``fetch_by_ids``).

    The ``redis`` client is duck-typed: anything providing ``get``,
    ``set(..., nx=, ex=)``, ``delete``, and ``exists`` with redis-py
    semantics works, which keeps tests independent of a live server.
    """

    name: str
    """Adapter identifier; also namespaces this adapter's cache keys."""

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

    def _ttl(self, key) -> int:
        raise NotImplementedError

    def _fetch_from_source(self, keys: list) -> dict:
        raise NotImplementedError

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
        JSON-serialisable data.
        """
        self._redis.set(self._cache_key(key), json.dumps(data), ex=self._ttl(key))

    def _acquire_lock(self, key) -> bool:
        return bool(self._redis.set(self._lock_key(key), "1", nx=True, ex=self.LOCK_TTL))

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

    Implements `fetch` as a Redis cache loop over the dayobs range;
    subclasses set ``name`` and implement `_fetch_from_source`.
    """

    name: str = "dayobs"
    """Identifier used as the key in ``Service.adapters`` dicts and
    to namespace this adapter's cache keys."""

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

    @abstractmethod
    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, Any]:
        """Fetch the given (cache-missing) dayobs from upstream.

        Parameters
        ----------
        dayobs_list : `list` [`int`]
            Only the dayobs that were not found in the cache. May be
            non-contiguous; adapters wrapping range-based upstream
            APIs should group it with `contiguous_runs` and issue one
            range request per run.

        Returns
        -------
        `dict` [`int`, `Any`]
            Processed, frontend-ready, JSON-serialisable data with an
            entry for every requested dayobs (an empty container for
            nights with no data).
        """
        raise NotImplementedError

    def _ttl(self, dayobs: int) -> int:
        """TTL for a dayobs entry: today's TTL for today, historic
        otherwise. Adapters with different lifetimes override this
        (or mix in `MutableDataMixin`)."""
        return TODAY_TTL_REDIS if dayobs == current_dayobs() else HISTORIC_TTL_REDIS

    def refresh(self, dayobs: int) -> None:
        """Refresh one dayobs' cache entry, fetch-then-overwrite.

        Fetches fresh data first and then replaces the old value with
        a single Redis ``SET`` — the existing entry is never deleted
        ahead of the fetch, so requests arriving mid-refresh are served
        the previous value instead of falling into a cold-miss window.
        If the fetch fails the old entry is left untouched.

        Bypasses the single-flight lock: it never leaves the cache
        empty, so there is no stampede to prevent, and the worst case
        against a racing cold fetch is one redundant upstream call.

        Called by ``RefreshWorker`` — every interval for today, and
        once more for the previous dayobs after rollover (the
        finalisation pass, which re-stores the completed night with
        its historical TTL).
        """
        data = self._fetch_from_source([dayobs])
        if dayobs not in data:
            raise KeyError(
                f"{type(self).__name__}._fetch_from_source did not return an entry for dayobs {dayobs}"
            )
        self._store(dayobs, data[dayobs])

    def refresh_today(self) -> None:
        """Refresh today's cache entry (see `refresh`)."""
        self.refresh(current_dayobs())


class IdCachedAdapter(CachedAdapter, ABC):
    """Base class for adapters keyed by opaque IDs rather than dayobs.

    Held directly by ``BlockDetailsService``. Runs the same cache loop
    as `DayobsCachedAdapter` (including single-flight locks), adapted for
    string keys, with a long fixed TTL. Subclasses set ``name`` and
    implement `_fetch_from_source`. Not registered with the
    ``RefreshWorker`` — there is no "today" entry to refresh.
    """

    name: str = "id_based"
    """Identifier used as the key in ``Service.adapters`` dicts and
    to namespace this adapter's cache keys."""

    def fetch_by_ids(self, ids: list[str]) -> dict[str, Any]:
        """Return records for the given IDs, keyed by ID.

        Cached IDs are served from Redis; only the misses are fetched
        upstream (as a single batch).
        """
        return self._fetch_cached(ids)

    @abstractmethod
    def _fetch_from_source(self, ids: list[str]) -> dict[str, Any]:
        """Fetch the given (cache-missing) IDs from upstream.

        Returns JSON-serialisable data with an entry for every
        requested ID.
        """
        raise NotImplementedError

    def _ttl(self, id_: str) -> int:
        """Fixed lifetime — ID-keyed data has no today/historic
        split. Adapters with mutable records override this."""
        return HISTORIC_TTL_REDIS


class InstrumentDayobsCachedAdapter(CachedAdapter, ABC):
    """Base class for adapters partitioned by instrument and dayobs.

    Cache keys are the composite ``"{instrument}:{dayobs}"`` — used when
    an upstream is per-instrument (so all instruments cannot share one
    dayobs key) but still dayobs-driven. Owns the cache mechanics only:
    subclasses provide ``_fetch_run(instrument, run_start, run_end)``
    whose rows each carry a ``day_obs`` field, and may add request
    validation by overriding `fetch`.
    """

    # Instruments this app serves: the RefreshWorker warms each for
    # today, and subclasses validate requests against this set.
    INSTRUMENTS = ("lsstcam", "latiss")

    def fetch(self, instrument: str, start_dayobs: int, end_dayobs: int) -> dict[int, list[dict]]:
        """Return rows for the range, partitioned by dayobs.

        Parameters
        ----------
        instrument : `str`
            Instrument name; lower-cased for the cache key.
        start_dayobs, end_dayobs : `int`
            Inclusive bounds of the range, in YYYYMMDD form.
        """
        instrument = instrument.lower()
        days = dayobs_range(start_dayobs, end_dayobs)
        by_key = self._fetch_cached([self._compose_key(instrument, day) for day in days])
        return {day: by_key[self._compose_key(instrument, day)] for day in days}

    def refresh(self, dayobs: int) -> None:
        """Warm ``dayobs`` for each instrument (called by RefreshWorker).

        Fetch-then-overwrite per instrument in `INSTRUMENTS`, so the
        served entry is never emptied. Instruments are independent: one
        instrument's upstream failure is logged and the rest still run.
        """
        for instrument in self.INSTRUMENTS:
            key = self._compose_key(instrument, dayobs)
            try:
                fetched = self._fetch_from_source([key])
                self._store(key, fetched[key])
            except Exception:
                logger.exception(f"{self.name} refresh failed for {instrument} dayobs {dayobs}")

    @staticmethod
    def _compose_key(instrument: str, dayobs: int) -> str:
        return f"{instrument}:{dayobs}"

    @staticmethod
    def _split_key(key: str) -> tuple[str, int]:
        instrument, dayobs = key.rsplit(":", 1)
        return instrument, int(dayobs)

    def _ttl(self, key: str) -> int:
        """Today's entry gets the short TTL, past nights the historic one."""
        _, dayobs = self._split_key(key)
        return TODAY_TTL_REDIS if dayobs == current_dayobs() else HISTORIC_TTL_REDIS

    def _fetch_from_source(self, keys: list[str]) -> dict[str, list[dict]]:
        results: dict[str, list[dict]] = {key: [] for key in keys}
        # Composite keys may span instruments, each with its own schema:
        # fetch one instrument's contiguous runs at a time, then bucket
        # the returned rows back by their day_obs.
        by_instrument: dict[str, list[int]] = defaultdict(list)
        for key in keys:
            instrument, dayobs = self._split_key(key)
            by_instrument[instrument].append(dayobs)
        for instrument, dayobs_list in by_instrument.items():
            for run_start, run_end in contiguous_runs(dayobs_list):
                logger.debug(f"Fetching {self.name} for {instrument} dayobs {run_start}..{run_end}")
                for record in self._fetch_run(instrument, run_start, run_end):
                    key = self._compose_key(instrument, record.get("day_obs"))
                    if key in results:
                        results[key].append(record)
        return results

    @abstractmethod
    def _fetch_run(self, instrument: str, run_start: int, run_end: int) -> list[dict]:
        """Return one instrument's rows for a contiguous dayobs run.

        Provided by a source mixin (e.g. `SqlClient`-backed). Each row
        must carry a ``day_obs`` so it can be bucketed to its cache key.
        """
        raise NotImplementedError
