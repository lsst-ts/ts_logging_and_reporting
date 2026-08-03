import os
import threading
import time

import pytest
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.services.worker_pool_mixin import WorkerPoolMixin

# Every function submitted below is a stdlib one. Workers import the
# function by module path to unpickle it, and the stdlib is importable
# everywhere, so the tests do not depend on this test module being
# importable inside a worker process.


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
        occupiers = [threading.Thread(target=occupy, args=(worker, 5)) for _ in range(slots)]
        for thread in occupiers:
            thread.start()
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    worker.run_in_worker(os.getpid)
                except HTTPException as error:
                    assert error.status_code == 503
                    break
                time.sleep(0.05)
            else:
                pytest.fail("pool admitted a request beyond its capacity")
        finally:
            for thread in occupiers:
                thread.join(timeout=30)

    def test_slot_is_released_after_a_failure(self, worker):
        worker.pool_timeout = 0.2
        for _ in range(3):
            with pytest.raises(HTTPException):
                worker.run_in_worker(time.sleep, 5)
        worker.pool_timeout = 10.0
        # A leaked permit per failure would have exhausted the slots.
        assert worker.run_in_worker(abs, -1) == 1


class TestWorkerDeath:
    def test_pool_survives_a_killed_worker(self, worker):
        worker.pool_timeout = 2.0
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
