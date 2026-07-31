import signal

import pytest

import lsst.ts.logging_and_reporting.run_refresh_worker as entrypoint


class StubAdapters:
    """Stands in for the adapters package.

    Every ``get_*_adapter()`` lookup returns a factory producing a
    uniquely named stub, and the order they were built in is recorded.
    """

    def __init__(self):
        self.created = []

    def __getattr__(self, name):
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

    def test_registers_every_adapter(self, harness):
        entrypoint.run_refresh_worker()
        created = harness.adapters.created
        assert len(created) == len(harness.worker.adapters)
        assert len(set(created)) == len(created)

    def test_exposures_adapter_precedes_visit_overhead(self, harness):
        # The overhead adapter reads the exposures cache, so a cycle
        # must refresh exposures first.
        entrypoint.run_refresh_worker()
        created = harness.adapters.created
        assert created.index("get_consdb_exposures_adapter") < created.index("get_visit_overhead_adapter")


class TestCachingDisabled:
    def test_no_worker_and_no_handlers(self, harness, monkeypatch):
        monkeypatch.setattr(entrypoint, "redis_caching_disabled", lambda: True)
        entrypoint.run_refresh_worker()
        assert harness.workers == []
        assert harness.handlers == {}
        assert harness.adapters.created == []
