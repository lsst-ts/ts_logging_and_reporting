#!/usr/bin/env python3
"""Before/after performance measurement for the backend refactor.

Running the backend under test
------------------------------
Start the API server with the package's console script (serves uvicorn
on port 8080)::

    run_logging_and_reporting

or equivalently::

    python -m lsst.ts.logging_and_reporting.run_logging_and_reporting

The ``after`` mode additionally needs the Redis instance the backend is
using, reachable by ``redis-cli`` (defaults: 127.0.0.1:6379).

Point this script at the backend directly (port 8080), never through
nginx, so HTTP-layer caching does not contaminate the numbers.

Modes
-----
baseline
    Capture pre-refactor timings. The current architecture has no
    server-side cache, so every request is effectively cold. Run this
    BEFORE the refactor starts and commit the JSON output (e.g. to
    doc/perf/baseline.json).

after
    Post-refactor cache-state scenarios (see "Scenarios" below).
    Flushes the backend's Redis between runs, so never point it at a
    shared deployment.

combine
    Join the per-scenario files in a working directory into one result
    file. ``combine WORK_DIR OUT [--tidy]``. This is run automatically
    by both baseline and after modes if --out is provided.

compare
    Join two result files and print a before/after markdown table.
    Because the baseline is all-cold, every delta reads as "change
    versus what users experience today". Join rules:

    - after 1-day non-burst scenarios    → baseline ``cold-1day``
    - after 1-day burst scenarios        → baseline ``cold-1day-burst``
    - after 7-day non-burst scenarios    → baseline ``cold-7day``
    - after 7-day burst scenarios        → baseline ``cold-7day-burst``
    - after /block-details non-burst     → baseline ``cold-all-keys``
    - after /block-details burst         → baseline ``cold-all-keys-burst``
      (both warn if the runs measured different key sets)

Output
------
Both run modes write one JSON file per scenario into ``--work-dir``
(default ``./perf_test_working``) as that scenario finishes, so a run
that dies part-way keeps everything it had already measured. Each file
carries the run metadata beside its own results and stands on its own.

Given ``--out``, the files are joined into a single result file at the
end and then deleted; ``--no-tidy`` keeps them. Without ``--out``
nothing is joined and the files are left for the combine mode.

``--skip-present`` skips any scenario whose file already exists, which
resumes an interrupted run. Without it, a non-empty working directory
prompts before overwriting.

Scenarios
---------
All scenarios issue N timed requests (``--runs``, default 50) with a
short pause before and between runs (``--pause``), against fixed
historical dayobs ranges derived from ``--day-start``: "1day" = day 1
only, "7day" = days 1-7. ``dayObsEnd`` is exclusive, so those are sent as
``dayObsStart=day1, dayObsEnd=day1+1`` and ``day1, day1+7``
respectively. Priming requests are untimed and re-issued after every
flush so the cache state is identical for each timed run.

Baseline mode (everything is cold by construction; these are the
reference numbers the after-scenarios are compared against):

``cold-1day`` / ``cold-7day``
    N timed requests for the 1-day and 7-day range.
``cold-1day-burst`` / ``cold-7day-burst``
    Burst rounds (see below) against each range — the pre-refactor
    concurrency reference.
``cold-all-keys`` (/block-details)
    N timed requests for all BLOCK keys.
``cold-all-keys-burst`` (/block-details)
    Burst rounds for all BLOCK keys — the pre-refactor
    concurrency reference for the key-driven endpoint.

After mode, per dayobs endpoint:

``cold-1day`` / ``cold-7day``
    FLUSHDB before every run, so every request misses the cache and
    fetches upstream. Worst case; should match the baseline within
    the Redis check/store overhead (~10%). A larger regression means
    the adapter layer added real cost.
``hot-1day`` / ``hot-7day``
    Prime the same request before each run (no flush), so every dayobs
    is cached. Steady state for today's data (the RefreshWorker keeps it
    warm) and for recently viewed ranges. Expect latency dominated by
    collation/serialisation; for the visualisation endpoints this
    measures the irreducible figure/PNG build cost. The pair separates
    the fixed part of that cost from the per-dayobs part: if hot-1day
    and hot-7day are close, the work is fixed (build and serialise) and
    caching prepared per-day frames would not help.
``partial-rolling-7day``
    FLUSHDB, prime days 1-7, measure days 2-8 (one missing day).
    Models the everyday case: the default week view shifting by one
    day after dayobs rollover. Expect roughly the cold 1-day cost —
    partial loads should scale with missing days, not range length.
``partial-extension-7day``
    FLUSHDB, prime day 1, measure days 1-7 (six missing days). Models
    a user expanding a single-night view to a week. Expect roughly the
    cold cost of the six missing days.

Burst scenarios fire ``--burst-size`` (default 10) simultaneous
requests per round, ``--bursts`` (default 30) rounds, with a longer
``--burst-pause`` (default 5 s) before and between rounds. Flushing/priming
happens once per round, mirroring the equivalent single-request
scenario. Per-request p50/p95 describe the typical user in the crowd;
``burst_wall_p50`` is the median time for a whole round to complete
(governed by the slowest request), so wall time near the single-request
latency means genuine parallelism, while wall time near burst_size x
single latency means requests are being serialised somewhere.

``cold-7day-burst``
    All requests miss the same keys at once — exposes cache-stampede
    behaviour (concurrent misses should not each fetch upstream).
``hot-1day-burst`` / ``hot-7day-burst``
    Fully cached concurrent load — several users opening the same
    digest page at the same time, for a single night and for a week.
``partial-rolling-7day-burst``
    Days 1-7 primed, concurrent requests for days 2-8: everyone
    missing the same single day at once. The most realistic stampede
    case — the first users each morning after rollover. (No
    partial-extension burst: extending a range is an individual
    action, never simultaneous across users.)

/block-details (key-driven, not dayobs-driven; keys come from
``DEFAULT_BLOCK_KEYS``, overridable with ``--block-keys``):

``cold-all-keys``
    FLUSHDB, request all keys — every key fetched from Jira/Zephyr.
``hot-all-keys``
    All keys primed — fully cached.
``partial-keys``
    First half of the keys primed, all keys requested. Checks that
    cost scales with the number of missing keys, not requested keys.
``cold-all-keys-burst`` / ``hot-all-keys-burst`` / ``partial-keys-burst``
    The same three under burst rounds. The frontend fans a whole batch
    of BLOCK keys out on page load, so concurrent demand for the same
    keys is the realistic shape for this endpoint; the cold burst is
    where the per-key single-flight lock has to stop N simultaneous
    requests becoming N upstream fetches.

Notes
-----
- Use a fixed, well-populated historical week (--day-start) and the
  same value for the baseline and after runs.
- Latency alone does not prove partial loads fetch only the missing
  days; verify upstream request counts via server logs or the
  CachedAdapter instrumentation point.

Examples
--------
    python scripts/perf_test.py baseline --day-start 20250601 \
        --out doc/perf/baseline.json
    python scripts/perf_test.py after --day-start 20250601 \
        --out doc/perf/after.json
    python scripts/perf_test.py compare doc/perf/baseline.json \
        doc/perf/after.json

Resuming a run that was interrupted, then joining it by hand:

    python scripts/perf_test.py after --day-start 20250601 --skip-present
    python scripts/perf_test.py combine ./perf_test_working \
        doc/perf/after.json --tidy
"""

import argparse
import concurrent.futures
import datetime as dt
import json
import math
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse

import requests

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
DEFAULT_DAY_START = 20260611
DEFAULT_RUNS = 50
DEFAULT_PAUSE = 1.0  # seconds between timed runs
DEFAULT_TIMEOUT = 120  # seconds
DEFAULT_BURST_SIZE = 10  # simultaneous requests per burst
DEFAULT_BURSTS = 30  # bursts per burst scenario
DEFAULT_BURST_PAUSE = 5.0  # seconds between bursts
DEFAULT_BLOCK_KEYS = [
    "BLOCK-407",
    "BLOCK-T523",
    "BLOCK-T523_6",
    "BLOCK-T539",
    "BLOCK-T594",
    "BLOCK-T664_a",
    "BLOCK-T664_b",
    "BLOCK-T682",
]

# Endpoint inventory. "instrument": whether the endpoint takes an
# instrument query param. /block-details is key-driven and handled
# separately (see run_block_details_scenarios).
ENDPOINTS = [
    {"name": "almanac", "path": "/almanac", "instrument": False},
    {"name": "narrative-log", "path": "/narrative-log", "instrument": True},
    {"name": "exposure-entries", "path": "/exposure-entries", "instrument": True},
    {"name": "exposures", "path": "/exposures", "instrument": True},
    {"name": "obs-status", "path": "/obs-status", "instrument": False},
    {"name": "multi-night-visit-maps", "path": "/multi-night-visit-maps", "instrument": True},
    {"name": "static-visit-map", "path": "/static-visit-map", "instrument": True},
]


DEFAULT_WORK_DIR = "./perf_test_working"
# Only the TCP connection matters, so any cheap path will do and the
# response, including a 404, is discarded.
CONNECTION_WARMUP_PATH = "/health"


def add_days(dayobs: int, days: int) -> int:
    """Return dayobs (YYYYMMDD int) shifted by the given number of days."""
    date = dt.datetime.strptime(str(dayobs), "%Y%m%d") + dt.timedelta(days=days)
    return int(date.strftime("%Y%m%d"))


def git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        ).stdout.strip()
    except Exception:
        return "unknown"


def sample_record(result: dict) -> dict:
    """The per-request fields kept in the saved results."""
    record = {
        "status": result["status"],
        "seconds": round(result["seconds"], 3),
        "bytes": result["bytes"],
    }
    if result.get("error"):
        record["error"] = result["error"]
    return record


class ScenarioStore:
    """One JSON file per scenario, written as each scenario finishes.

    Each file carries the run metadata alongside the scenario's own
    results, so a scenario stands on its own and files from separate
    invocations stay comparable. Names are prefixed with the mode
    because baseline and after share several scenario names.
    """

    def __init__(self, args, mode: str) -> None:
        self.work_dir = args.work_dir
        self.mode = mode
        self.skip_present = args.skip_present
        self.metadata = {
            "mode": mode,
            "git_commit": git_commit(),
            "base_url": args.base_url,
            "day_start": args.day_start,
            "instrument": args.instrument,
            "timeout": args.timeout,
            "runs_per_scenario": args.runs,
            "pause_between_runs": args.pause,
            "burst_size": args.burst_size,
            "bursts_per_scenario": args.bursts,
            "pause_between_bursts": args.burst_pause,
        }
        os.makedirs(self.work_dir, exist_ok=True)

    def path(self, endpoint: str, scenario: str) -> str:
        return os.path.join(self.work_dir, f"{self.mode}__{endpoint}__{scenario}.json")

    def present(self, endpoint: str, scenario: str) -> bool:
        """Whether this scenario already ran and should be skipped."""
        return self.skip_present and os.path.exists(self.path(endpoint, scenario))

    def write(self, endpoint: str, scenario: str, summary: dict) -> None:
        record = {
            "endpoint": endpoint,
            "scenario": scenario,
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            **self.metadata,
            **summary,
        }
        path = self.path(endpoint, scenario)
        # Write then rename, so an interrupted write cannot leave a
        # truncated file for --skip-present to trust.
        temp_path = f"{path}.partial"
        with open(temp_path, "w") as f:
            json.dump(record, f, indent=2)
        os.replace(temp_path, path)
        print(f"[{self.mode}] {endpoint} {scenario} written to {path}", flush=True)


def confirm_work_dir(work_dir: str, skip_present: bool) -> None:
    if skip_present or not os.path.isdir(work_dir):
        return
    existing = [name for name in os.listdir(work_dir) if name.endswith(".json")]
    if not existing:
        return
    print(f"{work_dir} already holds {len(existing)} scenario file(s), which will be overwritten.")
    print("Pass --skip-present to keep them and run only what is missing.")
    if input("Continue? Type 'yes': ").strip().lower() != "yes":
        sys.exit("Aborted.")


def combine_results(work_dir: str) -> list:
    """Every scenario file in the directory, as one results list."""
    if not os.path.isdir(work_dir):
        sys.exit(f"No such working directory: {work_dir}")
    names = sorted(name for name in os.listdir(work_dir) if name.endswith(".json"))
    if not names:
        sys.exit(f"No scenario files in {work_dir}")
    results = []
    for name in names:
        with open(os.path.join(work_dir, name)) as f:
            results.append(json.load(f))
    modes = {r.get("mode") for r in results}
    if len(modes) > 1:
        print(
            f"WARNING: {work_dir} mixes modes {sorted(str(m) for m in modes)}; "
            "the combined file covers both.",
            file=sys.stderr,
        )
    return results


def tidy_work_dir(work_dir: str) -> None:
    for name in os.listdir(work_dir):
        if name.endswith(".json"):
            os.remove(os.path.join(work_dir, name))


class Runner:
    def __init__(self, args):
        self.base_url = args.base_url.rstrip("/")
        self.timeout = args.timeout
        self.runs = args.runs
        self.pause = args.pause
        self.burst_size = args.burst_size
        self.bursts = args.bursts
        self.burst_pause = args.burst_pause
        self.session = requests.Session()
        self._thread_local = threading.local()
        self.redis_host = getattr(args, "redis_host", None)
        self.redis_port = getattr(args, "redis_port", None)

    def flush_redis(self):
        result = subprocess.run(
            ["redis-cli", "-h", self.redis_host, "-p", str(self.redis_port), "FLUSHDB"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or "OK" not in result.stdout:
            sys.exit(
                f"redis-cli FLUSHDB failed (host={self.redis_host}, "
                f"port={self.redis_port}): {result.stderr or result.stdout}"
            )

    def full_url(self, path: str, params) -> str:
        """The exact URL a request with these params queries."""
        return requests.Request("GET", f"{self.base_url}{path}", params=params).prepare().url

    def request(self, path: str, params: dict) -> dict:
        """One timed request. Returns seconds, size, and status."""
        start = time.perf_counter()
        try:
            response = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
            elapsed = time.perf_counter() - start
            return {
                "seconds": elapsed,
                "bytes": len(response.content),
                "status": response.status_code,
            }
        except requests.RequestException as exc:
            return {
                "seconds": time.perf_counter() - start,
                "bytes": 0,
                "status": None,
                "error": str(exc),
            }

    def _progress_start(self, label: str, done: int, total: int) -> None:
        """Open a line for one run, left unterminated until it finishes."""
        if not label:
            return
        print(f"{label} [{done}/{total}]", end="", flush=True)

    def _progress_done(self, label: str) -> None:
        if not label:
            return
        print(" DONE", flush=True)

    def _run_primes(self, prime: list | None) -> list:
        """Issue the untimed priming requests, returning any failures."""
        failures = []
        for prime_path, prime_params in prime or []:
            result = self.request(prime_path, prime_params)
            if result["status"] != 200:
                failures.append(result)
        return failures

    def _warn_first_prime_error(self, label: str, seen: list, new: list) -> None:
        """Warn once, on a scenario's first priming failure.

        Called after the progress line is terminated, so the warning
        cannot land mid-line, and only when these are the first failures
        seen — a prime that fails every run would otherwise warn once
        per run.
        """
        if not new or len(seen) != len(new):
            return
        detail = new[0].get("error") or f"HTTP {new[0]['status']}"
        print(
            f"WARNING: priming failed for {label.strip() or 'scenario'} ({detail}); "
            "the cache state is not what this scenario assumes",
            file=sys.stderr,
        )

    def _warn_prime_total(self, label: str, prime_errors: list) -> None:
        if len(prime_errors) > 1:
            print(
                f"WARNING: {len(prime_errors)} priming requests failed in total for "
                f"{label.strip() or 'scenario'}",
                file=sys.stderr,
            )

    def timed_runs(
        self,
        path: str,
        params: dict,
        flush_before_each: bool = False,
        prime: list | None = None,
        label: str = "",
    ) -> dict:
        """Run self.runs timed requests and summarise.

        prime: optional list of (path, params) requests to issue
        (untimed) before each timed run, after any flush. Used to set
        up partial-load cache states.
        """
        samples = []
        errors = []
        recorded = []
        prime_errors = []
        for i in range(self.runs):
            if self.pause:
                time.sleep(self.pause)
            self._progress_start(label, i + 1, self.runs)
            if flush_before_each:
                self.flush_redis()
            new_prime_errors = self._run_primes(prime)
            prime_errors.extend(new_prime_errors)
            result = self.request(path, params)
            if result["status"] == 200:
                samples.append(result)
            else:
                errors.append(result)
            recorded.append(sample_record(result))
            self._progress_done(label)
            self._warn_first_prime_error(label, prime_errors, new_prime_errors)
        self._warn_prime_total(label, prime_errors)
        return self._summarise(samples, errors, path, params, prime, recorded, prime_errors)

    def burst_runs(
        self,
        path: str,
        params: dict,
        flush_before_each: bool = False,
        prime: list | None = None,
        label: str = "",
    ) -> dict:
        """Run self.bursts rounds of self.burst_size simultaneous requests.

        Flushing/priming happens once per burst (mirroring the
        equivalent single-request scenario), and self.burst_pause
        seconds separate consecutive bursts. The worker pool is created
        once for the whole scenario so each thread's session, and so its
        connection, survives between rounds.
        """
        samples = []
        errors = []
        burst_walls = []
        rounds = []
        prime_errors = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.burst_size) as pool:
            self._open_connections(pool)
            for i in range(self.bursts):
                if self.burst_pause:
                    time.sleep(self.burst_pause)
                self._progress_start(label, i + 1, self.bursts)
                if flush_before_each:
                    self.flush_redis()
                new_prime_errors = self._run_primes(prime)
                prime_errors.extend(new_prime_errors)
                start = time.perf_counter()
                futures = [
                    pool.submit(self._concurrent_request, path, params) for _ in range(self.burst_size)
                ]
                results = [f.result() for f in futures]
                burst_walls.append(time.perf_counter() - start)
                for result in results:
                    if result["status"] == 200:
                        samples.append(result)
                    else:
                        errors.append(result)
                rounds.append([sample_record(result) for result in results])
                self._progress_done(label)
                self._warn_first_prime_error(label, prime_errors, new_prime_errors)
        self._warn_prime_total(label, prime_errors)
        summary = self._summarise(samples, errors, path, params, prime, rounds, prime_errors)
        recorded = summary.pop("samples")
        summary.update(
            burst_size=self.burst_size,
            bursts=self.bursts,
            burst_wall_p50=round(statistics.median(burst_walls), 3),
            burst_walls=[round(wall, 3) for wall in burst_walls],
            samples=recorded,
        )
        return summary

    def _open_connections(self, pool: concurrent.futures.ThreadPoolExecutor) -> None:
        """Open every worker thread's connection before the timed rounds.

        Each thread builds its session lazily, so without this the first
        round pays one handshake per thread and no later round does —
        concentrated in a single round, that lands in p95 and max rather
        than averaging out. A barrier holds each task until all of them
        have arrived, which is what forces every worker to be used.

        Warms against a trivial path: only the connection matters, and
        the response, including a 404, is discarded.
        """
        barrier = threading.Barrier(self.burst_size)

        def open_one():
            try:
                self._thread_session().get(f"{self.base_url}{CONNECTION_WARMUP_PATH}", timeout=self.timeout)
            except requests.RequestException:
                pass
            try:
                barrier.wait(timeout=self.timeout)
            except threading.BrokenBarrierError:
                pass

        for future in [pool.submit(open_one) for _ in range(self.burst_size)]:
            future.result()

    def _thread_session(self) -> requests.Session:
        """One session per burst worker thread.

        Sessions are not thread-safe, but a session per thread still
        keeps connections alive between rounds — without it every burst
        request pays a TCP handshake that the sequential scenarios,
        sharing one session, never do.
        """
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(self.session.headers)
            self._thread_local.session = session
        return session

    def _concurrent_request(self, path: str, params) -> dict:
        """As request(), but on this thread's own session."""
        start = time.perf_counter()
        try:
            response = self._thread_session().get(
                f"{self.base_url}{path}",
                params=params,
                timeout=self.timeout,
            )
            return {
                "seconds": time.perf_counter() - start,
                "bytes": len(response.content),
                "status": response.status_code,
            }
        except requests.RequestException as exc:
            return {
                "seconds": time.perf_counter() - start,
                "bytes": 0,
                "status": None,
                "error": str(exc),
            }

    def _summarise(
        self,
        samples: list,
        errors: list,
        path: str,
        params,
        prime: list | None,
        recorded: list,
        prime_errors: list,
    ) -> dict:
        summary = {"runs": len(samples), "errors": len(errors), "url": self.full_url(path, params)}
        if prime:
            summary["prime_urls"] = [self.full_url(p, pp) for p, pp in prime]
        if errors:
            summary["first_error"] = errors[0].get("error") or f"HTTP {errors[0]['status']}"
        if prime_errors:
            summary["prime_errors"] = len(prime_errors)
            summary["first_prime_error"] = prime_errors[0].get("error") or f"HTTP {prime_errors[0]['status']}"
        if samples:
            times = sorted(s["seconds"] for s in samples)
            sizes = [s["bytes"] for s in samples]
            summary.update(
                p50=round(statistics.median(times), 3),
                p95=round(times[math.ceil(0.95 * len(times)) - 1], 3),
                min=round(times[0], 3),
                max=round(times[-1], 3),
                bytes=statistics.mode(sizes),
            )
            if len(set(sizes)) > 1:
                summary["bytes_differ"] = True
        summary["samples"] = recorded
        return summary


def range_params(endpoint: dict, day_start: int, day_end: int, instrument: str) -> dict:
    """Query params for a dayobs range; day_end is exclusive."""
    params = {"dayObsStart": day_start, "dayObsEnd": day_end}
    if endpoint["instrument"]:
        params["instrument"] = instrument
    return params


def selected_endpoints(args):
    if not args.endpoints:
        return ENDPOINTS
    unknown = set(args.endpoints) - {e["name"] for e in ENDPOINTS}
    if unknown:
        sys.exit(f"Unknown endpoint(s): {', '.join(sorted(unknown))}")
    return [e for e in ENDPOINTS if e["name"] in args.endpoints]


def skip_notice(mode: str, endpoint: str, scenario: str) -> None:
    print(f"[{mode}] {endpoint} {scenario} — already present, skipped", flush=True)


def run_baseline(args, store: ScenarioStore) -> None:
    runner = Runner(args)
    day1 = args.day_start
    day2 = add_days(day1, 1)
    day8 = add_days(day1, 7)
    for endpoint in selected_endpoints(args):
        name = endpoint["name"]
        for label, (lo, hi) in {"cold-1day": (day1, day2), "cold-7day": (day1, day8)}.items():
            if store.present(name, label):
                skip_notice("baseline", name, label)
                continue
            params = range_params(endpoint, lo, hi, args.instrument)
            runner.request(endpoint["path"], params)  # warm-up (connections, lazy imports)
            summary = runner.timed_runs(endpoint["path"], params, label=f"[baseline] {name} {label}")
            store.write(name, label, summary)

        # No cache in the baseline, so the cold bursts are the whole
        # concurrency reference: one per range length, matching the
        # after-mode burst scenarios they are joined against.
        for label, (lo, hi) in {
            "cold-1day-burst": (day1, day2),
            "cold-7day-burst": (day1, day8),
        }.items():
            if store.present(name, label):
                skip_notice("baseline", name, label)
                continue
            params = range_params(endpoint, lo, hi, args.instrument)
            summary = runner.burst_runs(endpoint["path"], params, label=f"[baseline] {name} {label}")
            store.write(name, label, summary)

    # No server-side cache exists in the baseline, so one sequential and
    # one burst all-keys scenario capture /block-details (always cold).
    if not args.block_keys:
        print("[baseline] block-details skipped — no BLOCK keys given", flush=True)
        return
    all_keys = [("key", k) for k in args.block_keys]
    if store.present("block-details", "cold-all-keys"):
        skip_notice("baseline", "block-details", "cold-all-keys")
    else:
        runner.request("/block-details", all_keys)  # warm-up
        summary = runner.timed_runs(
            "/block-details", all_keys, label="[baseline] block-details cold-all-keys"
        )
        store.write("block-details", "cold-all-keys", summary)

    if store.present("block-details", "cold-all-keys-burst"):
        skip_notice("baseline", "block-details", "cold-all-keys-burst")
    else:
        summary = runner.burst_runs(
            "/block-details", all_keys, label="[baseline] block-details cold-all-keys-burst"
        )
        store.write("block-details", "cold-all-keys-burst", summary)


def run_after(args, store: ScenarioStore) -> None:
    runner = Runner(args)
    day1 = args.day_start
    day2 = add_days(day1, 1)
    day8 = add_days(day1, 7)
    day9 = add_days(day1, 8)
    for endpoint in selected_endpoints(args):
        name = endpoint["name"]
        path = endpoint["path"]
        p_1day = range_params(endpoint, day1, day2, args.instrument)
        p_7day = range_params(endpoint, day1, day8, args.instrument)
        p_shift = range_params(endpoint, day2, day9, args.instrument)

        scenarios = [
            # (label, params, flush_each, prime, burst)
            ("cold-1day", p_1day, True, None, False),
            ("cold-7day", p_7day, True, None, False),
            ("hot-1day", p_1day, False, [(path, p_1day)], False),
            ("hot-7day", p_7day, False, [(path, p_7day)], False),
            ("partial-rolling-7day", p_shift, True, [(path, p_7day)], False),
            ("partial-extension-7day", p_7day, True, [(path, p_1day)], False),
            ("cold-7day-burst", p_7day, True, None, True),
            ("hot-1day-burst", p_1day, False, [(path, p_1day)], True),
            ("hot-7day-burst", p_7day, False, [(path, p_7day)], True),
            ("partial-rolling-7day-burst", p_shift, True, [(path, p_7day)], True),
        ]
        pending = []
        for scenario in scenarios:
            if store.present(name, scenario[0]):
                skip_notice("after", name, scenario[0])
            else:
                pending.append(scenario)
        if not pending:
            continue

        # Warm up process-level state once; cold runs flush Redis anyway.
        print(f"[after] {name} warm-up ...", flush=True)
        runner.flush_redis()
        runner.request(path, p_7day)

        for label, params, flush_each, prime, burst in pending:
            run = runner.burst_runs if burst else runner.timed_runs
            summary = run(
                path,
                params,
                flush_before_each=flush_each,
                prime=prime,
                label=f"[after] {name} {label}",
            )
            store.write(name, label, summary)

    if args.block_keys:
        run_block_details_scenarios(runner, args.block_keys, store)
    else:
        print("[after] block-details skipped — no BLOCK keys given", flush=True)


def run_block_details_scenarios(runner: Runner, keys: list[str], store: ScenarioStore) -> None:
    """/block-details is ID-keyed: hot/cold/partial by keys, not days."""
    path = "/block-details"
    all_keys = [("key", k) for k in keys]
    subset = [("key", k) for k in keys[: max(1, len(keys) // 2)]]
    scenarios = [
        # (label, params, flush_each, prime, burst)
        ("cold-all-keys", all_keys, True, None, False),
        ("hot-all-keys", all_keys, False, [(path, all_keys)], False),
        ("partial-keys", all_keys, True, [(path, subset)], False),
        # The frontend fans a whole batch of BLOCK keys out on page load,
        # so concurrent demand for the same keys is the realistic shape
        # here, not the sequential one. Mirrors the three above.
        ("cold-all-keys-burst", all_keys, True, None, True),
        ("hot-all-keys-burst", all_keys, False, [(path, all_keys)], True),
        ("partial-keys-burst", all_keys, True, [(path, subset)], True),
    ]
    pending = []
    for scenario in scenarios:
        if store.present("block-details", scenario[0]):
            skip_notice("after", "block-details", scenario[0])
        else:
            pending.append(scenario)
    if not pending:
        return

    print("[after] block-details warm-up ...", flush=True)
    runner.flush_redis()
    runner.request(path, all_keys)

    for label, params, flush_each, prime, burst in pending:
        run = runner.burst_runs if burst else runner.timed_runs
        summary = run(
            path,
            params,
            flush_before_each=flush_each,
            prime=prime,
            label=f"[after] block-details {label}",
        )
        store.write("block-details", label, summary)


def completion(row: dict) -> str:
    """Share of a scenario's requests that returned 200.

    Percentiles are computed over successful requests only, so anything
    below 100% means they describe a subset — and a low rate means that
    subset is the requests that happened to be served first.
    """
    total = row.get("runs", 0) + row.get("errors", 0)
    if not total:
        return "—"
    return f"{100 * row.get('runs', 0) / total:.0f}%"


def print_markdown(results: list):
    print("\n| Endpoint | Scenario | completed | p50 (s) | p95 (s) | min | max | bytes | errors |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        if r.get("runs"):
            print(
                f"| {r['endpoint']} | {r['scenario']} | {completion(r)} | {r['p50']} | {r['p95']} "
                f"| {r['min']} | {r['max']} | {bytes_cell(r)} | {r['errors']} |"
            )
        else:
            print(
                f"| {r['endpoint']} | {r['scenario']} | {completion(r)} | — | — | — | — | — "
                f"| {r['errors']} ({r.get('first_error', '?')}) |"
            )


def save_results(results: list, out: str):
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as f:
        json.dump({"results": results}, f, indent=2)
    print(f"\nSaved {out}")
    print_markdown(results)


def wall_cells(base: dict, after: dict) -> tuple[str, str, str]:
    """The three wall-clock columns for one row.

    Only burst scenarios have a wall time: it is how long a whole burst
    of simultaneous requests took end to end, which is what a user
    hitting a cold dashboard actually waits. It is worth showing next to
    the per-request p50 because the two can move differently — the point
    of the single-flight lock is that concurrent requests for one key
    stop duplicating the upstream fetch, which shortens the burst
    without necessarily making any single request faster. Non-burst rows
    get empty cells.
    """
    if base.get("burst_wall_p50") is None or after.get("burst_wall_p50") is None:
        return "", "", ""
    return (
        cell(base, "burst_wall_p50"),
        cell(after, "burst_wall_p50"),
        change_cell(base, after, "burst_wall_p50"),
    )


# ``mode`` and ``git_commit`` are expected to differ between the two
# runs, so they are not compared. ``timestamp`` is handled separately.
COMPARED_METADATA = (
    "base_url",
    "day_start",
    "instrument",
    "timeout",
    "runs_per_scenario",
    "pause_between_runs",
    "burst_size",
    "bursts_per_scenario",
    "pause_between_bursts",
)

TIMESTAMP_TOLERANCE = dt.timedelta(hours=24)


def timestamps_far_apart(before: str | None, after: str | None) -> bool:
    """Whether two scenario timestamps are far enough apart to matter.

    A before/after pair is normally captured over days, so only a gap
    wide enough to suggest different upstream conditions is reported.
    """
    try:
        gap = dt.datetime.fromisoformat(after) - dt.datetime.fromisoformat(before)
    except (TypeError, ValueError):
        return False
    return abs(gap) > TIMESTAMP_TOLERANCE


def metadata_differences(base: dict, after: dict) -> list[tuple[str, str, str]]:
    """``(key, before, after)`` for each metadata value that differs."""
    differences = [
        (key, str(base.get(key)), str(after.get(key)))
        for key in COMPARED_METADATA
        if base.get(key) != after.get(key)
    ]
    if timestamps_far_apart(base.get("timestamp"), after.get("timestamp")):
        differences.append(("timestamp", str(base.get("timestamp")), str(after.get("timestamp"))))
    return differences


def warn_metadata(mismatches: dict[tuple[str, str, str], list[str]]) -> None:
    for (key, base_value, after_value), scenarios in mismatches.items():
        shown = ", ".join(scenarios[:3])
        if len(scenarios) > 3:
            shown += f", and {len(scenarios) - 3} more"
        print(
            f"WARNING: {key} differs ({base_value} -> {after_value}) "
            f"in {len(scenarios)} scenario(s): {shown}",
            file=sys.stderr,
        )


def timestamp_range(results: list) -> str:
    """``first to last`` over a run's scenarios, to the minute."""
    stamps = sorted(r["timestamp"] for r in results if r.get("timestamp"))
    if not stamps:
        return "unknown"
    first, last = stamps[0][:16], stamps[-1][:16]
    return first if first == last else f"{first} to {last}"


def commit_summary(results: list) -> str:
    commits = sorted({r["git_commit"] for r in results if r.get("git_commit")})
    return ", ".join(commits) if commits else "unknown"


def check_file_consistency(results: list, label: str, expected_mode: str) -> None:
    """Warn if a result file is the wrong mode, or spans several commits."""
    modes = {r.get("mode") for r in results}
    if modes - {expected_mode}:
        print(
            f"WARNING: the {label} file should hold only {expected_mode!r} scenarios, "
            f"but contains {sorted(str(mode) for mode in modes)}",
            file=sys.stderr,
        )
    commits = {r.get("git_commit") for r in results}
    if len(commits) > 1:
        print(
            f"WARNING: the {label} file spans {len(commits)} commits "
            f"({sorted(str(commit) for commit in commits)}) — its scenarios were not "
            "all run against the same code",
            file=sys.stderr,
        )


def request_keys(row: dict) -> list[str]:
    """The ``key`` query parameters of a scenario's request URL.

    Compared instead of the whole URL, which also carries the base URL —
    two runs against different hosts measure the same key set.
    """
    query = urllib.parse.urlparse(row.get("url", "")).query
    return sorted(urllib.parse.parse_qs(query).get("key", []))


def bytes_cell(row: dict, other: dict | None = None) -> str:
    """The scenario's modal response size, tagged where sizes vary.

    ``[DIFFERS]`` means the scenario's own responses were not all the
    same size; ``[DIFFERS AFTER]`` that the before and after runs
    settled on different sizes.
    """
    if row.get("bytes") is None:
        return "—"
    tags = ""
    if row.get("bytes_differ") or (other or {}).get("bytes_differ"):
        tags += " [DIFFERS]"
    if other is not None and other.get("bytes") != row.get("bytes"):
        tags += " [DIFFERS AFTER]"
    return f"{row['bytes']}{tags}"


def completed_cell(row: dict) -> str:
    """``ok/total``, so the denominator behind a percentile is visible."""
    runs, errors = row.get("runs", 0), row.get("errors", 0)
    return f"{runs}/{runs + errors}"


def cell(row: dict, key: str) -> str:
    value = row.get(key)
    return "—" if value is None else str(value)


def change_cell(base: dict, after: dict, key: str) -> str:
    """Percentage change, or ``—`` when either side has no value."""
    base_value, after_value = base.get(key), after.get(key)
    if base_value is None or after_value is None or not base_value:
        return "—"
    return f"{(after_value - base_value) / base_value * 100:+.0f}%"


def run_compare(args):
    with open(args.before) as f:
        before_results = json.load(f)["results"]
    with open(args.after) as f:
        after_results = json.load(f)["results"]
    check_file_consistency(before_results, "before", "baseline")
    check_file_consistency(after_results, "after", "after")

    before_map = {(r["endpoint"], r["scenario"]): r for r in before_results}
    after_by_endpoint = {}
    for r in after_results:
        after_by_endpoint.setdefault(r["endpoint"], []).append(r)

    before_endpoints = {endpoint for endpoint, _ in before_map}
    missing_endpoints = before_endpoints ^ set(after_by_endpoint)
    for endpoint in sorted(before_endpoints - set(after_by_endpoint)):
        print(f"WARNING: {endpoint} is in the before file but not the after file", file=sys.stderr)
    for endpoint in sorted(set(after_by_endpoint) - before_endpoints):
        print(f"WARNING: {endpoint} is in the after file but not the before file", file=sys.stderr)

    # Baseline requests are always cold, so every after-scenario joins to
    # the cold baseline of the same shape: matching range length (or key
    # set, for /block-details), and burst against burst. Several after
    # scenarios can share one baseline row.
    rows_to_print = []
    mismatches: dict[tuple[str, str, str], list[str]] = {}
    joined_before = set()
    for endpoint, rows in after_by_endpoint.items():
        for r in rows:
            suffix = "-burst" if "burst" in r["scenario"] else ""
            if endpoint == "block-details":
                base_scenario = f"cold-all-keys{suffix}"
            else:
                length = "1day" if "1day" in r["scenario"] else "7day"
                base_scenario = f"cold-{length}{suffix}"
            base = before_map.get((endpoint, base_scenario))
            if not base:
                if endpoint not in missing_endpoints:
                    print(
                        f"WARNING: no before scenario {base_scenario!r} to join "
                        f"{endpoint}/{r['scenario']} against; the row is omitted",
                        file=sys.stderr,
                    )
                continue
            joined_before.add((endpoint, base_scenario))
            scenario_name = f"{endpoint}/{r['scenario']}"
            if endpoint == "block-details" and request_keys(base) != request_keys(r):
                difference = ("block key set", str(request_keys(base)), str(request_keys(r)))
                mismatches.setdefault(difference, []).append(scenario_name)
            for row, side in ((base, "before"), (r, "after")):
                if row.get("prime_errors"):
                    print(
                        f"WARNING: {scenario_name} had {row['prime_errors']} failed priming "
                        f"request(s) on the {side} side ({row.get('first_prime_error')}); "
                        "its cache state was not what the scenario assumes",
                        file=sys.stderr,
                    )
            for difference in metadata_differences(base, r):
                mismatches.setdefault(difference, []).append(scenario_name)
            rows_to_print.append((endpoint, base, r))

    for endpoint, scenario in sorted(set(before_map) - joined_before):
        if endpoint not in missing_endpoints:
            print(
                f"WARNING: before scenario {endpoint}/{scenario} has no after scenario joining to it",
                file=sys.stderr,
            )
    warn_metadata(mismatches)

    print(f"\nBefore: {commit_summary(before_results)} ({timestamp_range(before_results)})")
    print(f"After:  {commit_summary(after_results)} ({timestamp_range(after_results)})")
    print(
        "\n| Endpoint | Scenario | completed (before → after) | before p50 (s) | after p50 (s) "
        "| change | before wall p50 (s) | after wall p50 (s) | wall change | bytes |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for endpoint, base, r in rows_to_print:
        wall_before, wall_after, wall_delta = wall_cells(base, r)
        print(
            f"| {endpoint} | {r['scenario']} "
            f"| {completed_cell(base)} → {completed_cell(r)} "
            f"| {cell(base, 'p50')} | {cell(r, 'p50')} | {change_cell(base, r, 'p50')} "
            f"| {wall_before} | {wall_after} | {wall_delta} | {bytes_cell(r, base)} |"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)

    def add_run_args(p):
        p.add_argument(
            "--base-url", default=DEFAULT_BASE_URL, help="Backend base URL — uvicorn directly, not nginx"
        )
        p.add_argument(
            "--day-start",
            type=int,
            default=DEFAULT_DAY_START,
            help="First dayobs of the fixed historical test week (YYYYMMDD)",
        )
        p.add_argument("--instrument", default="LSSTCam")
        p.add_argument("--runs", type=int, default=DEFAULT_RUNS, help="Timed runs per scenario")
        p.add_argument(
            "--pause", type=float, default=DEFAULT_PAUSE, help="Seconds to sleep between timed runs"
        )
        p.add_argument(
            "--burst-size", type=int, default=DEFAULT_BURST_SIZE, help="Simultaneous requests per burst"
        )
        p.add_argument("--bursts", type=int, default=DEFAULT_BURSTS, help="Bursts per burst scenario")
        p.add_argument(
            "--burst-pause", type=float, default=DEFAULT_BURST_PAUSE, help="Seconds to sleep between bursts"
        )
        p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
        p.add_argument("--endpoints", nargs="*", help="Subset of endpoint names to run (default: all)")
        p.add_argument(
            "--block-keys",
            nargs="*",
            default=DEFAULT_BLOCK_KEYS,
            help="BLOCK keys for /block-details scenarios",
        )
        p.add_argument(
            "--out",
            help="Combine the per-scenario files into this path; omit to leave them in --work-dir",
        )
        p.add_argument(
            "--work-dir",
            default=DEFAULT_WORK_DIR,
            help="Directory for per-scenario result files, written as each scenario finishes",
        )
        p.add_argument(
            "--skip-present",
            action="store_true",
            help="Skip scenarios whose result file already exists in --work-dir, to resume a run",
        )
        p.add_argument(
            "--no-tidy",
            action="store_true",
            help="Keep the per-scenario files after combining them into --out",
        )

    baseline = sub.add_parser("baseline", help="Capture pre-refactor timings")
    add_run_args(baseline)

    after = sub.add_parser("after", help="Post-refactor cache-state scenarios")
    add_run_args(after)
    after.add_argument("--redis-host", default="127.0.0.1")
    after.add_argument("--redis-port", type=int, default=6379)

    combine = sub.add_parser("combine", help="Join per-scenario files into one result file")
    combine.add_argument("work_dir", help="Directory of per-scenario result files")
    combine.add_argument("out", help="Write the combined JSON results here")
    combine.add_argument(
        "--tidy", action="store_true", help="Delete the per-scenario files after combining them"
    )

    compare = sub.add_parser("compare", help="Diff two result files")
    compare.add_argument("before")
    compare.add_argument("after")

    args = parser.parse_args()
    if args.mode == "compare":
        run_compare(args)
        return
    if args.mode == "combine":
        save_results(combine_results(args.work_dir), args.out)
        if args.tidy:
            tidy_work_dir(args.work_dir)
        return

    confirm_work_dir(args.work_dir, args.skip_present)
    store = ScenarioStore(args, args.mode)
    if args.mode == "baseline":
        run_baseline(args, store)
    else:
        run_after(args, store)
    if not args.out:
        print(f"\nPer-scenario files are in {args.work_dir}; join them with the combine mode.")
        return
    save_results(combine_results(args.work_dir), args.out)
    if not args.no_tidy:
        tidy_work_dir(args.work_dir)


if __name__ == "__main__":
    main()
