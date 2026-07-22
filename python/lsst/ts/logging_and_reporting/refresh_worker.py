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

"""Background worker that keeps today's cache entries warm.

The worker's fetch-then-
overwrite cycle (via ``DayobsCachedAdapter.refresh``) means today's entry —
the stampede-prone hot key — never misses under normal operation, and
user requests for today never trigger an external fetch directly.

The refresh loop runs on a daemon thread because adapters are
synchronous by design (see the plan's "Sync vs Async" decision):
blocking upstream fetches on a thread leave uvicorn's event loop free,
whereas an asyncio task would have to push every refresh into an
executor thread anyway.

Three deployment/rollover behaviours matter here:

- The first refresh cycle runs immediately on ``start()``, so today's
  entries are warm right after a deploy or restart instead of staying
  cold for one interval.
- When the astronomical dayobs rolls over (12:00 UTC), the worker
  refreshes the *previous* dayobs one final time before resuming.
  This finalisation pass fetches the complete, now-immutable night
  and stores it with the long historical TTL — without it, yesterday's
  entry would expire ~one short-TTL after rollover (a guaranteed daily
  cold miss on the most-viewed historical night), possibly caching a
  version truncated at the worker's last pre-rollover refresh.
- A Redis leader lease ensures only one worker refreshes per cycle
  even when several processes run the app (``uvicorn --workers``,
  multiple k8s replicas). Duplicate refreshes would be harmless
  (fetch-then-overwrite is idempotent) but waste upstream calls. If
  the leader dies, its lease expires and another instance takes over
  within a cycle.
"""

import logging
import threading
import uuid
from typing import Any

from lsst.ts.logging_and_reporting.utils import current_dayobs

from .adapters.base_adapters import DayobsCachedAdapter, InstrumentDayobsCachedAdapter
from .cache_ttl import TODAY_TTL

logger = logging.getLogger(__name__)

LEADER_LOCK_KEY = "lock:refresh_worker:leader"


class RefreshWorker:
    """Daemon thread refreshing today's entry on each adapter.

    Every ``interval_seconds``, the leader instance calls
    ``refresh(today)`` on each registered `DayobsCachedAdapter`
    (fetch-then-overwrite, so the existing entry stays served during
    the refresh), plus a one-time finalisation refresh of the previous
    dayobs after rollover. Failures are logged per adapter without
    aborting the loop.

    Parameters
    ----------
    adapters : `list` [`DayobsCachedAdapter` | `InstrumentDayobsCachedAdapter`]
        The adapters to refresh. ``IdCachedAdapter`` instances are not
        accepted — they have no "today" entry.
    redis : `Any`
        redis-py-compatible client, used for the leader lease.
    interval_seconds : `int`, optional
        Seconds between refresh cycles. Defaults to `TODAY_TTL` — the
        cache-control ``max-age`` served for today's data — so
        clients are never staler than one refresh cycle.
    """

    def __init__(
        self,
        adapters: list[DayobsCachedAdapter | InstrumentDayobsCachedAdapter],
        redis: Any,
        interval_seconds: int = TODAY_TTL,
    ):
        self._adapters = list(adapters)
        self._redis = redis
        self._interval = interval_seconds
        # Lease must outlive the interval so leadership is stable
        # across cycles, but expire soon after a dead leader's last
        # re-up so failover happens within about one cycle.
        self._lease_ttl = interval_seconds * 2
        self._instance_id = uuid.uuid4().hex
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_today: int | None = None

    def start(self) -> None:
        """Start the daemon thread. Called once at application startup.

        The first refresh cycle runs immediately (on the worker
        thread, so startup is not blocked).
        """
        if self._thread is not None and self._thread.is_alive():
            logger.warning("RefreshWorker.start() called while already running")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="RefreshWorker", daemon=True)
        self._thread.start()
        logger.info(
            f"RefreshWorker started: {len(self._adapters)} adapter(s), "
            f"interval {self._interval}s, instance {self._instance_id}"
        )

    def stop(self) -> None:
        """Signal the thread to stop cleanly. Called at shutdown.

        Releases the leader lease if held, so another instance can
        take over immediately rather than waiting out the lease.
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval)
            self._thread = None
        self._release_leadership()
        logger.info("RefreshWorker stopped")

    def _run(self) -> None:
        # Immediate first cycle, then one per interval. wait() returns
        # True when stop() sets the event, ending the loop; a False
        # return means the interval elapsed normally.
        self._refresh_cycle()
        while not self._stop_event.wait(self._interval):
            self._refresh_cycle()

    def _refresh_cycle(self) -> None:
        """One pass: if leader, finalise the previous dayobs on
        rollover, then refresh today on every adapter.

        Never raises — a failure here (e.g. transient Redis
        connectivity in the leadership check) is logged and the cycle
        retried at the next interval, so the worker thread cannot die.
        """
        try:
            if not self._acquire_leadership():
                logger.debug("RefreshWorker: not leader this cycle, skipping")
                return
            today = current_dayobs()
            if self._last_today is not None and today != self._last_today:
                logger.info(
                    f"RefreshWorker: dayobs rollover {self._last_today} -> {today}; "
                    f"finalising {self._last_today}"
                )
                self._refresh_all(self._last_today)
            self._refresh_all(today)
            self._last_today = today
        except Exception:
            logger.exception("RefreshWorker: refresh cycle failed")

    def _refresh_all(self, dayobs: int) -> None:
        for adapter in self._adapters:
            if self._stop_event.is_set():
                return
            try:
                adapter.refresh(dayobs)
            except Exception:
                logger.exception(
                    f"RefreshWorker: refresh of dayobs {dayobs} failed for adapter {adapter.name!r}"
                )

    def _acquire_leadership(self) -> bool:
        """Acquire or re-up the leader lease; return True if leading.

        ``SET NX EX`` wins an unheld lease; the current holder renews
        by overwriting its own value. Between the read and the renewal
        an expired lease could be won by another instance and then
        overwritten here — that costs at worst one doubly-refreshed
        cycle, which fetch-then-overwrite makes harmless.
        """
        if self._redis.set(LEADER_LOCK_KEY, self._instance_id, nx=True, ex=self._lease_ttl):
            return True
        holder = self._redis.get(LEADER_LOCK_KEY)
        if holder is not None and self._decode(holder) == self._instance_id:
            self._redis.set(LEADER_LOCK_KEY, self._instance_id, ex=self._lease_ttl)
            return True
        return False

    def _release_leadership(self) -> None:
        holder = self._redis.get(LEADER_LOCK_KEY)
        if holder is not None and self._decode(holder) == self._instance_id:
            self._redis.delete(LEADER_LOCK_KEY)

    @staticmethod
    def _decode(value: Any) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)
