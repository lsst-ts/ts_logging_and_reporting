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

import signal

import pytest

import lsst.ts.logging_and_reporting.run_refresh_worker as entrypoint


class StubAdapters:
    """Stands in for the adapters package.

    `REFRESH_ADAPTERS` holds factories producing a uniquely named stub
    instead of an adapter, and the order they were built in is recorded.
    What the real tuple contains is asserted in the adapters' own tests.
    """

    NAMES = ("first", "second", "third")

    def __init__(self):
        self.created = []
        self.REFRESH_ADAPTERS = tuple(self._factory(name) for name in self.NAMES)

    def _factory(self, name):
        def factory():
            self.created.append(name)
            return f"<{name}>"

        return factory


class StubWorker:
    """Stands in for RefreshWorker, recording run/stop instead of
    touching Redis or the network."""

    def __init__(self, adapters):
        self.adapters = adapters
        self.ran = False
        self.stopped = False

    def run(self):
        self.ran = True

    def stop(self):
        self.stopped = True


class Harness:
    """The stubs swapped into the entrypoint for one call."""

    def __init__(self, adapters):
        self.adapters = adapters
        self.handlers = {}
        self.workers = []

    @property
    def worker(self):
        assert len(self.workers) == 1, f"expected one worker, got {len(self.workers)}"
        return self.workers[0]


@pytest.fixture
def harness(monkeypatch):
    stub_adapters = StubAdapters()
    harness = Harness(stub_adapters)

    def make_worker(adapters):
        worker = StubWorker(adapters)
        harness.workers.append(worker)
        return worker

    monkeypatch.setattr(entrypoint, "adapters", stub_adapters)
    monkeypatch.setattr(entrypoint, "RefreshWorker", make_worker)
    monkeypatch.setattr(entrypoint, "redis_caching_disabled", lambda: False)

    def record_handler(signum, handler):
        harness.handlers[signum] = handler

    # Capture registrations instead of touching this process's real
    # signal disposition.
    monkeypatch.setattr(signal, "signal", record_handler)
    return harness


class TestSignalHandling:
    def test_installs_sigterm_and_sigint_handlers(self, harness):
        entrypoint.run_refresh_worker()
        assert set(harness.handlers) == {signal.SIGTERM, signal.SIGINT}

    @pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
    def test_handler_stops_the_worker(self, harness, signum):
        entrypoint.run_refresh_worker()
        assert not harness.worker.stopped
        harness.handlers[signum](signum, None)
        assert harness.worker.stopped

    def test_handlers_are_installed_before_the_worker_runs(self, harness, monkeypatch):
        # Otherwise a signal arriving during the immediate first cycle
        # would hit the default disposition and kill the process.
        installed_when_run = []

        def make_worker(adapters):
            worker = StubWorker(adapters)
            worker.run = lambda: installed_when_run.append(set(harness.handlers))
            harness.workers.append(worker)
            return worker

        monkeypatch.setattr(entrypoint, "RefreshWorker", make_worker)
        entrypoint.run_refresh_worker()
        assert installed_when_run == [{signal.SIGTERM, signal.SIGINT}]


class TestWorkerSetup:
    def test_runs_the_worker(self, harness):
        entrypoint.run_refresh_worker()
        assert harness.worker.ran

    def test_builds_every_registered_adapter(self, harness):
        entrypoint.run_refresh_worker()
        assert harness.adapters.created == list(StubAdapters.NAMES)

    def test_the_worker_gets_them_in_registration_order(self, harness):
        entrypoint.run_refresh_worker()
        assert harness.worker.adapters == [f"<{name}>" for name in StubAdapters.NAMES]


class TestCachingDisabled:
    def test_no_worker_and_no_handlers(self, harness, monkeypatch):
        monkeypatch.setattr(entrypoint, "redis_caching_disabled", lambda: True)
        entrypoint.run_refresh_worker()
        assert harness.workers == []
        assert harness.handlers == {}
        assert harness.adapters.created == []
