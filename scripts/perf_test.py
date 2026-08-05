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

compare
    Join two result files and print a before/after markdown table.
    Because the baseline is all-cold, every delta reads as "change
    versus what users experience today". Join rules:

    - after ``cold-1day``                → baseline ``cold-1day``
    - after 7-day non-burst scenarios    → baseline ``cold-7day``
    - after 7-day burst scenarios        → baseline ``cold-7day-burst``
    - after /block-details non-burst     → baseline ``cold-all-keys``
    - after /block-details burst         → baseline ``cold-all-keys-burst``
      (both warn if the runs measured different key sets)

Scenarios
---------
All scenarios issue N timed requests (``--runs``, default 50) with a
short pause between runs (``--pause``), against fixed historical dayobs
ranges derived from ``--day-start``: "1day" = day 1 only, "7day" =
days 1-7. ``dayObsEnd`` is exclusive, so those are sent as
``dayObsStart=day1, dayObsEnd=day1+1`` and ``day1, day1+7``
respectively. Priming requests are untimed and re-issued after every
flush so the cache state is identical for each timed run.

Baseline mode (everything is cold by construction; these are the
reference numbers the after-scenarios are compared against):

``cold-1day`` / ``cold-7day``
    N timed requests for the 1-day and 7-day range.
``cold-7day-burst``
    Burst rounds (see below) against the 7-day range — the
    pre-refactor concurrency reference.
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
``hot-7day``
    Prime the same 7-day request before each run (no flush), so every
    dayobs is cached. Steady state for today's data (the RefreshWorker
    keeps it warm) and for recently viewed ranges. Expect latency
    dominated by collation/serialisation; for the visualisation
    endpoints this measures the irreducible figure/PNG build cost.
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
``--burst-pause`` (default 5 s) between rounds. Flushing/priming
happens once per round, mirroring the equivalent single-request
scenario. Per-request p50/p95 describe the typical user in the crowd;
``burst_wall_p50`` is the median time for a whole round to complete
(governed by the slowest request), so wall time near the single-request
latency means genuine parallelism, while wall time near burst_size x
single latency means requests are being serialised somewhere.

``cold-7day-burst``
    All requests miss the same keys at once — exposes cache-stampede
    behaviour (concurrent misses should not each fetch upstream).
``hot-7day-burst``
    Fully cached concurrent load — several users opening the same
    digest page at the same time.
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
import time

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
        if args.token:
            self.session.headers["Authorization"] = f"Bearer {args.token}"
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

    def _progress(self, label: str, done: int, total: int) -> None:
        """One line per run, left unterminated so `_progress_done` can
        append to the last of them."""
        if not label:
            return
        newline = "" if done == 1 else "\n"
        print(f"{newline}{label} [{done}/{total}]", end="", flush=True)

    def _progress_done(self, label: str) -> None:
        if not label:
            return
        print(" DONE", flush=True)

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
        for i in range(self.runs):
            if i > 0 and self.pause:
                time.sleep(self.pause)
            if flush_before_each:
                self.flush_redis()
            for prime_path, prime_params in prime or []:
                self.request(prime_path, prime_params)
            result = self.request(path, params)
            if result["status"] == 200:
                samples.append(result)
            else:
                errors.append(result)
            recorded.append(sample_record(result))
            self._progress(label, i + 1, self.runs)
        self._progress_done(label)
        return self._summarise(samples, errors, path, params, prime, recorded)

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
        seconds separate consecutive bursts. Each concurrent request
        uses its own connection rather than the shared session.
        """
        samples = []
        errors = []
        burst_walls = []
        rounds = []
        for i in range(self.bursts):
            if i > 0 and self.burst_pause:
                time.sleep(self.burst_pause)
            if flush_before_each:
                self.flush_redis()
            for prime_path, prime_params in prime or []:
                self.request(prime_path, prime_params)
            start = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.burst_size) as pool:
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
            self._progress(label, i + 1, self.bursts)
        self._progress_done(label)
        summary = self._summarise(samples, errors, path, params, prime, rounds)
        recorded = summary.pop("samples")
        summary.update(
            burst_size=self.burst_size,
            bursts=self.bursts,
            burst_wall_p50=round(statistics.median(burst_walls), 3),
            burst_walls=[round(wall, 3) for wall in burst_walls],
            samples=recorded,
        )
        return summary

    def _concurrent_request(self, path: str, params) -> dict:
        """As request(), but on a fresh connection (sessions are not
        thread-safe)."""
        start = time.perf_counter()
        try:
            response = requests.get(
                f"{self.base_url}{path}",
                params=params,
                headers=dict(self.session.headers),
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
        self, samples: list, errors: list, path: str, params, prime: list | None, recorded: list
    ) -> dict:
        summary = {"runs": len(samples), "errors": len(errors), "url": self.full_url(path, params)}
        if prime:
            summary["prime_urls"] = [self.full_url(p, pp) for p, pp in prime]
        if errors:
            summary["first_error"] = errors[0].get("error") or f"HTTP {errors[0]['status']}"
        if samples:
            times = sorted(s["seconds"] for s in samples)
            summary.update(
                p50=round(statistics.median(times), 3),
                p95=round(times[math.ceil(0.95 * len(times)) - 1], 3),
                min=round(times[0], 3),
                max=round(times[-1], 3),
                bytes=samples[0]["bytes"],
            )
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


def run_baseline(args) -> list:
    runner = Runner(args)
    day1 = args.day_start
    day2 = add_days(day1, 1)
    day8 = add_days(day1, 7)
    results = []
    for endpoint in selected_endpoints(args):
        for label, (lo, hi) in {"cold-1day": (day1, day2), "cold-7day": (day1, day8)}.items():
            params = range_params(endpoint, lo, hi, args.instrument)
            runner.request(endpoint["path"], params)  # warm-up (connections, lazy imports)
            summary = runner.timed_runs(
                endpoint["path"], params, label=f"[baseline] {endpoint['name']} {label}"
            )
            results.append({"endpoint": endpoint["name"], "scenario": label, **summary})

        # No cache in the baseline, so one burst scenario covers concurrency.
        params = range_params(endpoint, day1, day8, args.instrument)
        summary = runner.burst_runs(
            endpoint["path"], params, label=f"[baseline] {endpoint['name']} cold-7day-burst"
        )
        results.append({"endpoint": endpoint["name"], "scenario": "cold-7day-burst", **summary})

    # No server-side cache exists in the baseline, so one sequential and
    # one burst all-keys scenario capture /block-details (always cold).
    if args.block_keys:
        all_keys = [("key", k) for k in args.block_keys]
        runner.request("/block-details", all_keys)  # warm-up
        summary = runner.timed_runs(
            "/block-details", all_keys, label="[baseline] block-details cold-all-keys"
        )
        results.append({"endpoint": "block-details", "scenario": "cold-all-keys", **summary})

        summary = runner.burst_runs(
            "/block-details", all_keys, label="[baseline] block-details cold-all-keys-burst"
        )
        results.append({"endpoint": "block-details", "scenario": "cold-all-keys-burst", **summary})
    else:
        print("[baseline] block-details skipped — no BLOCK keys given", flush=True)
    return results


def run_after(args) -> list:
    runner = Runner(args)
    day1 = args.day_start
    day2 = add_days(day1, 1)
    day8 = add_days(day1, 7)
    day9 = add_days(day1, 8)
    results = []
    for endpoint in selected_endpoints(args):
        path = endpoint["path"]
        p_1day = range_params(endpoint, day1, day2, args.instrument)
        p_7day = range_params(endpoint, day1, day8, args.instrument)
        p_shift = range_params(endpoint, day2, day9, args.instrument)

        # Warm up process-level state once; cold runs flush Redis anyway.
        print(f"[after] {endpoint['name']} warm-up ...", flush=True)
        runner.flush_redis()
        runner.request(path, p_7day)

        scenarios = [
            # (label, params, flush_each, prime, burst)
            ("cold-1day", p_1day, True, None, False),
            ("cold-7day", p_7day, True, None, False),
            ("hot-7day", p_7day, False, [(path, p_7day)], False),
            ("partial-rolling-7day", p_shift, True, [(path, p_7day)], False),
            ("partial-extension-7day", p_7day, True, [(path, p_1day)], False),
            ("cold-7day-burst", p_7day, True, None, True),
            ("hot-7day-burst", p_7day, False, [(path, p_7day)], True),
            ("partial-rolling-7day-burst", p_shift, True, [(path, p_7day)], True),
        ]
        for label, params, flush_each, prime, burst in scenarios:
            run = runner.burst_runs if burst else runner.timed_runs
            summary = run(
                path,
                params,
                flush_before_each=flush_each,
                prime=prime,
                label=f"[after] {endpoint['name']} {label}",
            )
            results.append({"endpoint": endpoint["name"], "scenario": label, **summary})

    if args.block_keys:
        results.extend(run_block_details_scenarios(runner, args.block_keys))
    else:
        print("[after] block-details skipped — no BLOCK keys given", flush=True)
    return results


def run_block_details_scenarios(runner: Runner, keys: list[str]) -> list:
    """/block-details is ID-keyed: hot/cold/partial by keys, not days."""
    path = "/block-details"
    all_keys = [("key", k) for k in keys]
    subset = [("key", k) for k in keys[: max(1, len(keys) // 2)]]
    results = []
    print("[after] block-details warm-up ...", flush=True)
    runner.flush_redis()
    runner.request(path, all_keys)
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
    for label, params, flush_each, prime, burst in scenarios:
        run = runner.burst_runs if burst else runner.timed_runs
        summary = run(
            path,
            params,
            flush_before_each=flush_each,
            prime=prime,
            label=f"[after] block-details {label}",
        )
        results.append({"endpoint": "block-details", "scenario": label, **summary})
    return results


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
                f"| {r['min']} | {r['max']} | {r['bytes']} | {r['errors']} |"
            )
        else:
            print(
                f"| {r['endpoint']} | {r['scenario']} | {completion(r)} | — | — | — | — | — "
                f"| {r['errors']} ({r.get('first_error', '?')}) |"
            )


def save_results(args, mode: str, results: list):
    payload = {
        "mode": mode,
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "git_commit": git_commit(),
        "base_url": args.base_url,
        "day_start": args.day_start,
        "instrument": args.instrument,
        "runs_per_scenario": args.runs,
        "pause_between_runs": args.pause,
        "burst_size": args.burst_size,
        "bursts_per_scenario": args.bursts,
        "pause_between_bursts": args.burst_pause,
        "results": results,
    }
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"\nSaved {args.out}")
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
    before_wall = base.get("burst_wall_p50")
    after_wall = after.get("burst_wall_p50")
    if before_wall is None or after_wall is None:
        return "", "", ""
    delta = (after_wall - before_wall) / before_wall * 100
    return str(before_wall), str(after_wall), f"{delta:+.0f}%"


def run_compare(args):
    with open(args.before) as f:
        before = json.load(f)
    with open(args.after) as f:
        after = json.load(f)
    if before.get("day_start") != after.get("day_start"):
        print(
            f"WARNING: different dayobs ranges (before={before.get('day_start')}, "
            f"after={after.get('day_start')}) — numbers are not directly comparable.",
            file=sys.stderr,
        )
    before_map = {(r["endpoint"], r["scenario"]): r for r in before["results"] if r.get("runs")}
    after_by_endpoint = {}
    for r in after["results"]:
        if r.get("runs"):
            after_by_endpoint.setdefault(r["endpoint"], []).append(r)

    print(f"\nBefore: {before['git_commit']} ({before['timestamp']})")
    print(f"After:  {after['git_commit']} ({after['timestamp']})")
    print(
        "\n| Endpoint | Scenario | completed (before/after) | before p50 (s) | after p50 (s) "
        "| change | before wall p50 (s) | after wall p50 (s) | wall change |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for endpoint, rows in after_by_endpoint.items():
        # Baseline requests are always cold, so every after-scenario joins to
        # the cold baseline of the same shape: matching range length (or key
        # set, for /block-details), and burst against burst.
        for r in rows:
            suffix = "-burst" if "burst" in r["scenario"] else ""
            if endpoint == "block-details":
                base = before_map.get((endpoint, f"cold-all-keys{suffix}"))
                if base and base.get("url") != r.get("url"):
                    print(
                        "WARNING: block-details key sets differ between runs "
                        f"({base.get('url')} vs {r.get('url')})",
                        file=sys.stderr,
                    )
            else:
                length = "1day" if "1day" in r["scenario"] else "7day"
                base = before_map.get((endpoint, f"cold-{length}{suffix}"))
            if not base:
                continue
            delta = (r["p50"] - base["p50"]) / base["p50"] * 100
            wall_before, wall_after, wall_delta = wall_cells(base, r)
            print(
                f"| {endpoint} | {r['scenario']} | {completion(base)} / {completion(r)} "
                f"| {base['p50']} | {r['p50']} | {delta:+.0f}% "
                f"| {wall_before} | {wall_after} | {wall_delta} |"
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
        p.add_argument(
            "--token",
            default=os.environ.get("PERF_TEST_TOKEN"),
            help="Bearer token (default: $PERF_TEST_TOKEN); omit if the "
            "backend resolves tokens from its own environment",
        )
        p.add_argument("--endpoints", nargs="*", help="Subset of endpoint names to run (default: all)")
        p.add_argument(
            "--block-keys",
            nargs="*",
            default=DEFAULT_BLOCK_KEYS,
            help="BLOCK keys for /block-details scenarios",
        )
        p.add_argument("--out", help="Write JSON results to this path")

    baseline = sub.add_parser("baseline", help="Capture pre-refactor timings")
    add_run_args(baseline)

    after = sub.add_parser("after", help="Post-refactor cache-state scenarios")
    add_run_args(after)
    after.add_argument("--redis-host", default="127.0.0.1")
    after.add_argument("--redis-port", type=int, default=6379)

    compare = sub.add_parser("compare", help="Diff two result files")
    compare.add_argument("before")
    compare.add_argument("after")

    args = parser.parse_args()
    if args.mode == "baseline":
        save_results(args, "baseline", run_baseline(args))
    elif args.mode == "after":
        save_results(args, "after", run_after(args))
    else:
        run_compare(args)


if __name__ == "__main__":
    main()
