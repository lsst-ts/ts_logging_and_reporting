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

The worker runs as its own process (see ``run_refresh_worker.py``),
separate from the API service. Exactly one instance must run per
deployment: duplicate refreshes would be harmless (fetch-then-overwrite
is idempotent) but waste upstream calls, so the deployment — a single
``refresh-worker`` container in docker-compose, a single-replica
deployment in Kubernetes — is what guarantees uniqueness.

Two deployment/rollover behaviours matter here:

- The first refresh cycle runs immediately on ``run()``, so today's
  entries are warm right after a deploy or restart instead of staying
  cold for one interval.
- When the astronomical dayobs rolls over (12:00 UTC), the worker
  refreshes the *previous* dayobs one final time before resuming.
  This finalisation pass fetches the complete, now-immutable night
  and stores it with the long historical TTL — without it, yesterday's
  entry would expire ~one short-TTL after rollover (a guaranteed daily
  cold miss on the most-viewed historical night), possibly caching a
  version truncated at the worker's last pre-rollover refresh.
"""

import logging
import threading

from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

from .adapters.base_adapters import DayobsCachedAdapter, InstrumentDayobsCachedAdapter
from .cache_ttl import TODAY_TTL

logger = logging.getLogger(__name__)


class RefreshWorker:
    """Refresh loop keeping today's entry warm on each adapter.

    Every ``interval_seconds``, `run` calls ``refresh(today)`` on each
    registered `DayobsCachedAdapter` (fetch-then-overwrite, so the
    existing entry stays served during the refresh), plus a one-time
    finalisation refresh of the previous dayobs after rollover.
    Failures are logged per adapter without aborting the loop.

    Parameters
    ----------
    adapters : `list` [`DayobsCachedAdapter` | `InstrumentDayobsCachedAdapter`]
        The adapters to refresh. ``IdCachedAdapter`` instances are not
        accepted — they have no "today" entry.
    interval_seconds : `int`, optional
        Seconds between refresh cycles. Defaults to `TODAY_TTL` — the
        cache-control ``max-age`` served for today's data — so
        clients are never staler than one refresh cycle.
    """

    def __init__(
        self,
        adapters: list[DayobsCachedAdapter | InstrumentDayobsCachedAdapter],
        interval_seconds: int = TODAY_TTL,
    ):
        self._adapters = list(adapters)
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._last_today: int | None = None

    def run(self) -> None:
        """Run refresh cycles until `stop` is called.

        Blocks the calling thread; the worker process has nothing else
        to do. The first cycle runs immediately, further cycles once
        per interval.
        """
        logger.info(f"RefreshWorker started: {len(self._adapters)} adapter(s), interval {self._interval}s")
        # wait() returns True when stop() sets the event, ending the
        # loop; a False return means the interval elapsed normally.
        self._refresh_cycle()
        while not self._stop_event.wait(self._interval):
            self._refresh_cycle()
        logger.info("RefreshWorker stopped")

    def stop(self) -> None:
        """Signal the loop to finish, from a signal handler or thread.

        `run` returns once the in-flight cycle completes.
        """
        self._stop_event.set()

    def _refresh_cycle(self) -> None:
        """One pass: finalise the previous dayobs on rollover, then
        refresh today on every adapter.

        Never raises — a failure here is logged and the cycle retried
        at the next interval, so the loop cannot die.
        """
        try:
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
