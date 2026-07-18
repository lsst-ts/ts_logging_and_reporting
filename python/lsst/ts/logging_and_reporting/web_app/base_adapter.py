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

import datetime as dt
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from lsst.ts.logging_and_reporting.utils import current_dayobs

logger = logging.getLogger(__name__)

SECONDS_PER_DAY = 86400


def _dayobs_to_date(dayobs: int) -> dt.date:
    return dt.datetime.strptime(str(dayobs), "%Y%m%d").date()


def _date_to_dayobs(date: dt.date) -> int:
    return int(date.strftime("%Y%m%d"))


def dayobs_range(start_dayobs: int, end_dayobs: int) -> list[int]:
    """Enumerate all dayobs in ``[start_dayobs, end_dayobs]`` inclusive.

    Parameters
    ----------
    start_dayobs : `int`
        First dayobs of the range, in YYYYMMDD form.
    end_dayobs : `int`
        Last dayobs of the range (inclusive), in YYYYMMDD form.

    Returns
    -------
    `list` [`int`]
        Every dayobs in the range, ascending.
    """
    start = _dayobs_to_date(start_dayobs)
    end = _dayobs_to_date(end_dayobs)
    days = []
    while start <= end:
        days.append(_date_to_dayobs(start))
        start += dt.timedelta(days=1)
    return days


def contiguous_runs(dayobs_list: list[int]) -> list[tuple[int, int]]:
    """Group dayobs into contiguous ``(start, end)`` runs (inclusive).

    For adapters whose upstream API takes a min/max dayobs range: the
    cache loop hands ``_fetch_from_source`` only the missing dayobs,
    which may be non-contiguous, and issuing one range request per
    contiguous run avoids refetching the cached days in between.
    Adjacency is calendar-aware (20250131 and 20250201 are adjacent).

    Parameters
    ----------
    dayobs_list : `list` [`int`]
        Dayobs in YYYYMMDD form, in any order.

    Returns
    -------
    `list` [`tuple` [`int`, `int`]]
        Inclusive ``(start_dayobs, end_dayobs)`` per run, ascending.
        e.g. ``[20250101, 20250103, 20250104]`` →
        ``[(20250101, 20250101), (20250103, 20250104)]``.
    """
    if not dayobs_list:
        return []
    dates = sorted(_dayobs_to_date(d) for d in set(dayobs_list))
    runs = []
    run_start = run_end = dates[0]
    for date in dates[1:]:
        if date - run_end == dt.timedelta(days=1):
            run_end = date
        else:
            runs.append((_date_to_dayobs(run_start), _date_to_dayobs(run_end)))
            run_start = run_end = date
    runs.append((_date_to_dayobs(run_start), _date_to_dayobs(run_end)))
    return runs


class _SingleFlightCache:
    """Redis-backed cache loop with per-key single-flight locks.

    Internal machinery shared by `CachedAdapter` (integer dayobs keys)
    and `IdBasedAdapter` (string ID keys). Subclasses set ``name`` and
    provide ``_ttl`` and ``_fetch_from_source``.

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


class BaseAdapter(ABC):
    """Abstract interface all dayobs-driven adapters implement."""

    name: str = "base"
    """Identifier used as the key in ``Service.adapters`` dicts and
    to namespace this adapter's cache keys."""

    @abstractmethod
    def fetch(self, start_dayobs: int, end_dayobs: int) -> dict[int, Any]:
        """Return data for the dayobs range, partitioned by dayobs.

        The returned data is already processed — no further
        transformation is needed by the service layer.

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
        raise NotImplementedError


class CachedAdapter(_SingleFlightCache, BaseAdapter, ABC):
    """Base class for all dayobs-driven adapters.

    Implements `BaseAdapter.fetch` as a Redis cache loop over the
    dayobs range; subclasses set ``name`` and implement
    `_fetch_from_source`.
    """

    SHORT_TTL = 900
    """TTL (seconds) for today's entry.

    Must comfortably exceed the RefreshWorker interval so today's
    entry cannot expire between refresh cycles; the worker overwrites
    the entry in place every interval regardless of remaining TTL, so
    this does not increase staleness — it only bounds how stale the
    entry can get if the worker stalls entirely.
    """

    LONG_TTL = 30 * SECONDS_PER_DAY
    """TTL (seconds) for historical entries."""

    def fetch(self, start_dayobs: int, end_dayobs: int) -> dict[int, Any]:
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
        """TTL for a dayobs entry: short for today, long otherwise.

        Adapters whose historical data is still mutable (exposure log
        and narrative log entries can be added or edited for past
        nights) should override this to return `SHORT_TTL` for all
        dayobs, mirroring the always-short endpoint list in
        `CacheControlMiddleware`.
        """
        return self.SHORT_TTL if dayobs == current_dayobs() else self.LONG_TTL

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
        the long historical TTL).
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


class IdBasedAdapter(_SingleFlightCache, ABC):
    """Base class for adapters keyed by opaque IDs rather than dayobs.

    Held directly by ``BlockDetailsService``. Runs the same cache loop
    as `CachedAdapter` (including single-flight locks), adapted for
    string keys, with a long fixed TTL. Subclasses set ``name`` and
    implement `_fetch_from_source`. Not registered with the
    ``RefreshWorker`` — there is no "today" entry to refresh.
    """

    name: str = "id_based"
    """Identifier used as the key in ``Service.adapters`` dicts and
    to namespace this adapter's cache keys."""

    TTL = 30 * SECONDS_PER_DAY
    """Fixed TTL (seconds) for all entries."""

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
        return self.TTL
