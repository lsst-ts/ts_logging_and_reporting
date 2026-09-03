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

import os
import threading
import time

import pytest
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.services.worker_pool_mixin import WorkerPoolMixin
from lsst.ts.logging_and_reporting.utils.logging_config import (
    NO_TRACE_ID,
    current_trace_id,
    set_trace_id,
)

# Every function submitted below comes from the stdlib or this app's
# own package. Workers import the function by module path to unpickle
# it, and both are importable there, so the tests do not depend on this
# test module being importable inside a worker process.


class Worker(WorkerPoolMixin):
    pool_workers = 2
    pool_queue = 1
    pool_timeout = 10.0


def occupy(worker, seconds):
    """Hold one slot for `seconds`, whatever the outcome."""
    try:
        worker.run_in_worker(time.sleep, seconds)
    except HTTPException:
        pass


@pytest.fixture
def worker():
    service = Worker()
    yield service
    service.shutdown_worker_pool()


class TestIsolation:
    def test_runs_in_another_process(self, worker):
        assert worker.run_in_worker(os.getpid) != os.getpid()

    def test_returns_the_result(self, worker):
        assert worker.run_in_worker(abs, -3) == 3

    def test_each_service_gets_its_own_pool(self, worker):
        other = Worker()
        try:
            mine = {worker.run_in_worker(os.getpid) for _ in range(4)}
            theirs = {other.run_in_worker(os.getpid) for _ in range(4)}
            assert mine.isdisjoint(theirs)
        finally:
            other.shutdown_worker_pool()


class TestTraceId:
    """The calling request's ID reaches the worker process."""

    @pytest.fixture(autouse=True)
    def clear_trace_id(self):
        yield
        set_trace_id(NO_TRACE_ID)

    def test_the_worker_runs_under_the_callers_id(self, worker):
        set_trace_id("abc12345")
        assert worker.run_in_worker(current_trace_id) == "abc12345"

    def test_a_call_outside_a_request_gets_the_marker(self, worker):
        assert worker.run_in_worker(current_trace_id) == NO_TRACE_ID

    def test_the_id_does_not_persist_into_the_next_call(self, worker):
        # Workers are reused, so each call has to re-establish the ID
        # rather than inherit whatever the last one left behind.
        set_trace_id("abc12345")
        assert worker.run_in_worker(current_trace_id) == "abc12345"
        set_trace_id("def67890")
        assert worker.run_in_worker(current_trace_id) == "def67890"

    def test_the_result_still_comes_back_unchanged(self, worker):
        set_trace_id("abc12345")
        assert worker.run_in_worker(abs, -3) == 3


class TestLimits:
    def test_timeout_raises_504(self, worker):
        worker.pool_timeout = 0.2
        with pytest.raises(HTTPException) as excinfo:
            worker.run_in_worker(time.sleep, 5)
        assert excinfo.value.status_code == 504

    def test_saturation_sheds_with_503(self, worker):
        # Occupy every slot (workers plus queue), then confirm the next
        # request is refused rather than queued. The occupiers take
        # their slots inside run_in_worker, which cannot be observed
        # from here, so retry until they have or the deadline passes.
        slots = worker.pool_workers + worker.pool_queue
        occupiers = [threading.Thread(target=occupy, args=(worker, 2)) for _ in range(slots)]
        for thread in occupiers:
            thread.start()
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    worker.run_in_worker(os.getpid)
                except HTTPException as error:
                    assert error.status_code == 503
                    break
                time.sleep(0.01)
            else:
                pytest.fail("pool admitted a request beyond its capacity")
        finally:
            for thread in occupiers:
                thread.join(timeout=30)

    def test_slot_is_released_after_a_failure(self, worker):
        # Enough failures to exhaust every slot if a permit leaked. The
        # sleep is short because a timed-out call keeps its worker until
        # it finishes on its own, and this is about the semaphore rather
        # than how fast the pool drains.
        worker.pool_timeout = 0.1
        for _ in range(worker.pool_workers + worker.pool_queue):
            with pytest.raises(HTTPException):
                worker.run_in_worker(time.sleep, 0.5)
        worker.pool_timeout = 30.0
        # A leaked permit would make this a 503 rather than a result.
        assert worker.run_in_worker(abs, -1) == 1


class TestWorkerDeath:
    def test_pool_survives_a_killed_worker(self, worker):
        worker.pool_timeout = 0.3
        # os._exit skips cleanup, so the worker dies without reporting a
        # result: the request waits out its deadline.
        with pytest.raises(HTTPException) as excinfo:
            worker.run_in_worker(os._exit, 1)
        assert excinfo.value.status_code == 504

        worker.pool_timeout = 30.0
        assert worker.run_in_worker(abs, -2) == 2


class TestLifecycle:
    def test_start_is_idempotent(self, worker):
        pool, slots = worker.start_worker_pool()
        again, again_slots = worker.start_worker_pool()
        assert again is pool
        assert again_slots is slots

    def test_shutdown_then_start_builds_a_new_pool(self, worker):
        before = worker.run_in_worker(os.getpid)
        worker.shutdown_worker_pool()
        assert worker.run_in_worker(os.getpid) != before
