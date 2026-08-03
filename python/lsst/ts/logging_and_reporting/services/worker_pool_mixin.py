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

"""Worker-process offload for CPU-bound service work."""

import logging
import multiprocessing
import threading
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)

BUSY_DETAIL = "Service busy, try again shortly"
TIMEOUT_DETAIL = "Operation timed out"


class WorkerPoolMixin:
    """Runs one service's heavy work in its own pool of processes.

    Mixed in ahead of `Service` by services whose work is CPU-bound and
    so gains nothing from a thread. Each service gets its own pool, so
    one endpoint cannot starve another and each pool's workers import
    only what that service needs.

    Subclasses set the class attributes below and call `run_in_worker`.
    The application starts each pool on startup and releases it with
    `shutdown_worker_pool`.

    Attributes
    ----------
    pool_workers : `int`
        Worker processes. Kept small; the work is memory-heavy.
    pool_queue : `int`
        Requests allowed to wait beyond those already running. Past
        that the endpoint sheds load rather than occupying threadpool
        workers that other endpoints need.
    pool_timeout : `float`
        Seconds to wait for one call before giving up on it.
    pool_preload : `tuple` [`str`]
        Modules the forkserver imports before forking workers, so the
        import cost is paid once rather than per worker.
    """

    pool_workers: int = 4
    pool_queue: int = 4
    pool_timeout: float = 180.0
    pool_preload: tuple[str, ...] = ()

    # Guards pool creation only. Class-level so the mixin needs no
    # __init__: `self._pool` reads this None until an instance assigns
    # its own, so each service still ends up with a distinct pool.
    _pool_guard = threading.Lock()
    _pool: Any = None
    _pool_slots: threading.Semaphore | None = None

    def run_in_worker(self, func: Callable[..., Any], *args: Any) -> Any:
        """Run ``func(*args)`` in a worker process and return the result.

        ``func`` must be importable by name in the worker (a module
        level function) and its arguments and return value must pickle.

        A worker killed mid-call is replaced by the pool, so only the
        request it was serving is lost: that one waits out `pool_timeout`
        and gets a 504.

        Parameters
        ----------
        func : `callable`
            The function to run in a worker process.
        *args
            Positional arguments for ``func``.

        Returns
        -------
        result : `Any`
            Whatever ``func`` returned.

        Raises
        ------
        fastapi.HTTPException
            503 when the pool is already at capacity, 504 when the call
            exceeded `pool_timeout`.
        """
        pool, slots = self.start_worker_pool()
        name = type(self).__name__

        if not slots.acquire(blocking=False):
            logger.warning(f"{name} worker pool saturated, shedding request")
            raise HTTPException(status_code=503, detail=BUSY_DETAIL)
        try:
            return pool.apply_async(func, args).get(timeout=self.pool_timeout)
        except multiprocessing.TimeoutError as e:
            logger.error(f"{name} worker call exceeded {self.pool_timeout}s")
            raise HTTPException(status_code=504, detail=TIMEOUT_DETAIL) from e
        finally:
            slots.release()

    def start_worker_pool(self) -> tuple[Any, threading.Semaphore]:
        """Return this service's pool, starting it if it is not running.

        Called on application startup so that no request pays the cost
        of importing this service's dependencies into the forkserver.
        """
        with self._pool_guard:
            if self._pool is None:
                context = multiprocessing.get_context("forkserver")
                if self.pool_preload:
                    context.set_forkserver_preload(list(self.pool_preload))
                logger.info(f"{type(self).__name__} starting {self.pool_workers} workers")
                self._pool = context.Pool(self.pool_workers)
                self._pool_slots = threading.Semaphore(self.pool_workers + self.pool_queue)
            return self._pool, self._pool_slots

    def shutdown_worker_pool(self) -> None:
        """Stop this service's workers, abandoning any call in flight."""
        with self._pool_guard:
            pool, self._pool, self._pool_slots = self._pool, None, None
        if pool is not None:
            pool.terminate()
