# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""Background worker that keeps today's cache entries warm.

The worker's fetch-then- overwrite cycle
(via ``[Instrument]DayobsCachedAdapter.refresh``) means today's
entry never misses under normal operation, and user requests for
today never trigger an external fetch directly.

The worker runs as its own process (see ``run_refresh_worker.py``),
separate from the API service.

When the astronomical dayobs rolls over (12:00 UTC), the worker
refreshes the *previous* dayobs one final time to ensure the
full previous dayobs is captured.
"""

import logging
import threading
import time
import uuid

from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs
from lsst.ts.logging_and_reporting.utils.logging_config import (
    NO_TRACE_ID,
    set_trace_id,
)

from .adapters.base_adapters import DayobsCachedAdapter, InstrumentDayobsCachedAdapter
from .cache_ttl import TODAY_TTL_CLIENT

logger = logging.getLogger(__name__)

SLOW_CYCLE_FRACTION = 0.5
"""Fraction of the refresh interval a cycle may take before it is
logged as slow."""


class RefreshWorker:
    """Refresh loop keeping today's entry warm on each adapter.

    Every ``interval_seconds``, `run` calls ``refresh(today)`` on each
    registered `[Instrument]DayobsCachedAdapter` (fetch-then-overwrite, so the
    existing entry stays served during the refresh), plus a one-time
    finalisation refresh of the previous dayobs after rollover.
    Failures are logged per adapter without aborting the loop.

    Parameters
    ----------
    adapters : `list` [`DayobsCachedAdapter` | `InstrumentDayobsCachedAdapter`]
        The adapters to refresh. ``IdCachedAdapter`` instances are not
        accepted — they have no sense of "today"
    interval_seconds : `int`, optional
        Seconds between the start of consecutive refresh cycles. Defaults
        to `TODAY_TTL_CLIENT` — the cache-control ``max-age`` served for
        today's data — so clients are never staler than one refresh cycle.
    """

    def __init__(
        self,
        adapters: list[DayobsCachedAdapter | InstrumentDayobsCachedAdapter],
        interval_seconds: int = TODAY_TTL_CLIENT,
    ):
        self._adapters = list(adapters)
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._last_today: int | None = None

    def run(self) -> None:
        """Run refresh cycles until `stop` is called.

        The first cycle runs immediately; each subsequent cycle
        starts one interval after the previous one started, by waiting
        out only the part of the interval the cycle did not consume. A
        cycle that overruns the interval leaves no wait at all, so the
        next one starts immediately.
        """
        logger.info(f"RefreshWorker started: {len(self._adapters)} adapter(s), interval {self._interval}s")
        # wait() returns True when stop() sets the event, ending the
        # loop; a False return means the interval elapsed normally.
        elapsed = self._refresh_cycle()
        while not self._stop_event.wait(max(0.0, self._interval - elapsed)):
            elapsed = self._refresh_cycle()
        set_trace_id(NO_TRACE_ID)
        logger.info("RefreshWorker stopped")

    def stop(self) -> None:
        """Signal the loop to finish, from a signal handler or thread.

        `_refresh_all` checks the event before each adapter, so the
        refresh in progress completes, the remaining adapters in the
        cycle are skipped, and `run` returns.
        """
        self._stop_event.set()

    def _refresh_cycle(self) -> float:
        """Refresh today on every adapter.

        Never raises — a failure here is logged and the cycle retried
        at the next interval, so the loop cannot die. Finalises
        (re-fetches) yesterday, if required.

        Returns the seconds the pass took
        """
        # The same ID a request carries, so everything one cycle logs —
        # including the adapters it drives — is attributable to it.
        set_trace_id(uuid.uuid4().hex[:8])
        logger.info("RefreshWorker: refresh cycle started")
        started = time.monotonic()
        successes = 0
        failures = 0
        try:
            today = current_dayobs()
            if self._last_today is not None and today > self._last_today:
                logger.info(
                    f"RefreshWorker: dayobs rollover {self._last_today} -> {today}; "
                    f"finalising {self._last_today}"
                )
                ok, failed = self._refresh_all(self._last_today)
                successes += ok
                failures += failed
            ok, failed = self._refresh_all(today)
            successes += ok
            failures += failed
            self._last_today = today
        except Exception:
            logger.exception("RefreshWorker: refresh cycle failed")
        elapsed = time.monotonic() - started
        logger.info(
            f"RefreshWorker: refresh cycle finished in {elapsed:.1f}s "
            f"({successes} succeeded, {failures} failed)"
        )
        if elapsed > SLOW_CYCLE_FRACTION * self._interval:
            logger.warning(
                f"RefreshWorker: cycle took {elapsed:.1f}s, over "
                f"{SLOW_CYCLE_FRACTION:.0%} of the {self._interval}s interval"
            )
        return elapsed

    def _refresh_all(self, dayobs: int) -> tuple[int, int]:
        """Refresh every adapter for ``dayobs``.

        Returns the number of adapters that refreshed successfully and
        the number that raised
        """
        successes = 0
        failures = 0
        for adapter in self._adapters:
            if self._stop_event.is_set():
                break
            try:
                adapter.refresh(dayobs)
                successes += 1
            except Exception:
                failures += 1
                logger.exception(
                    f"RefreshWorker: refresh of dayobs {dayobs} failed for adapter {adapter.name!r}"
                )
        return successes, failures
