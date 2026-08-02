#!/usr/bin/env python3
"""Before/after endpoint output capture for the backend refactor.

Calls every data endpoint, writes one JSON file per call, and diffs two
capture directories. The point is to demonstrate that the refactor did
not change what the API returns.

Running the backend under test
------------------------------
Start the API server with the package's console script (serves uvicorn
on port 8080)::

    run_logging_and_reporting

Point this script at the backend directly (port 8080), never through
nginx, or you may capture a proxy-cached response rather than a live
one. The post-refactor run does not need the refresh worker: every
request is served from the adapter cache or fetched on demand, and
either way the payload is the same.

Usage
-----
::

    # on the pre-refactor checkout
    python scripts/capture_endpoints.py capture --out review/capture/before

    # on experiemental/cache-refactor
    python scripts/capture_endpoints.py capture --out review/capture/after

    python scripts/capture_endpoints.py compare \\
        review/capture/before review/capture/after

``compare`` exits non-zero if anything differs, so it can gate a script.

What gets called
----------------
Every dayobs endpoint is called twice, for a 1-day and a 7-day range
derived from ``--day-start``. ``dayObsEnd`` is exclusive, matching the
frontend and ``perf_test.py``, so those are sent as
``dayObsStart=day1, dayObsEnd=day1+1`` and ``day1, day1+7``.

Endpoints taking an instrument are called once per instrument
(``--instruments``, default LSSTCam and LATISS), so those are four calls
each. ``/block-details`` is key-driven rather than dayobs-driven and is
called once with a fixed set of known-good BLOCK keys
(``--block-keys``), covering both the Jira (``BLOCK-nnn``) and Zephyr
(``BLOCK-Tnnn``, ``BLOCK-Tnnn_x``) patterns. They were harvested from
real data rather than invented — see ``DEFAULT_BLOCK_KEYS`` — because
keys that resolve to nothing would match on both sides while testing
nothing. If you change ``--day-start``, revisit them.

``/obs-status`` is called with metrics requested, since the metric
arithmetic is the part of it worth diffing. ``/health`` and ``/version``
are skipped: one is constant and the other is expected to differ.

Comparability
-------------
Use a **fully historical** ``--day-start``. A range including today
returns different data on each run and will diff against itself.

Two things are deliberately canonicalised before writing, because they
differ between runs of identical code and would otherwise drown real
differences:

- **Bokeh model ids** in ``/multi-night-visit-maps``. Bokeh allocates
  ids from a per-process counter, so they depend on how many figures
  the process built earlier, not on the data. They are renumbered in
  first-seen order.
- **Base64 image payloads** in ``/static-visit-map``, which are replaced
  by a SHA-256 of the decoded bytes plus a length. Matplotlib embeds
  metadata in PNGs, so two renderings of identical data are not always
  byte-identical; a differing hash is worth investigating but is not by
  itself proof of a data change.

Every transform applied to a file is recorded in ``manifest.json``.
Nothing else is normalised: JSON object key order is sorted on write
because it carries no meaning, but list order is left alone, since
record ordering is part of the contract.

Caveats
-------
``/expected-exposures`` reads the simulation archive, which only keeps
simulations within 60 days of the night. If the before and after runs
are far enough apart in wall-clock time, a night can fall out of that
window between them and the endpoint will legitimately start returning
404. Capture both sides close together, or expect that one file to
differ for a reason that is not the refactor.
"""

import argparse
import base64
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
# Densest day with data on exposure-flags, exposure-entries and jira-tickets.
DEFAULT_DAY_START = 20260712
DEFAULT_INSTRUMENTS = ["LSSTCam", "LATISS"]
DEFAULT_TIMEOUT = 300  # seconds; visit-map endpoints can be slow

# Known-good keys, discovered from the science_program values of the
# default test week's /data-log records during the pre-refactor baseline
# run (review/BEFORE.json, 2026-07-29) and confirmed to resolve: 50 calls,
# no errors, a 2123-byte response. Hardcoded rather than rediscovered so
# both sides of a comparison ask for exactly the same keys.
#
# BLOCK-Tnnn resolves against Zephyr Scale and BLOCK-nnn against Jira, so
# the set covers both sources. It also covers suffixed variants, which are
# stored under their parent key upstream: _a and _b of a parent that never
# appears bare, and _6 of a parent that does.
#
# Change these together with --day-start. Keys are derived from what was
# observed in a specific week, and a different week will reference
# different BLOCKs.
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

# kind: "dayobs" = called for a 1-day and a 7-day range;
#       "block"  = called once with the fixed key set.
ENDPOINTS = [
    {"name": "almanac", "path": "/almanac", "kind": "dayobs", "instrument": False},
    {"name": "context-feed", "path": "/context-feed", "kind": "dayobs", "instrument": False},
    {"name": "expected-exposures", "path": "/expected-exposures", "kind": "dayobs", "instrument": False},
    {"name": "night-reports", "path": "/night-reports", "kind": "dayobs", "instrument": False},
    {
        "name": "obs-status",
        "path": "/obs-status",
        "kind": "dayobs",
        "instrument": False,
        "extra": {
            "includeEntries": "true",
            "includeIntervals": "true",
            "metric": ["fault", "weather", "idle"],
        },
    },
    {"name": "data-log", "path": "/data-log", "kind": "dayobs", "instrument": True},
    {"name": "exposure-entries", "path": "/exposure-entries", "kind": "dayobs", "instrument": True},
    {"name": "exposure-flags", "path": "/exposure-flags", "kind": "dayobs", "instrument": True},
    {"name": "exposures", "path": "/exposures", "kind": "dayobs", "instrument": True},
    {"name": "jira-tickets", "path": "/jira-tickets", "kind": "dayobs", "instrument": True},
    {"name": "narrative-log", "path": "/narrative-log", "kind": "dayobs", "instrument": True},
    {
        "name": "multi-night-visit-maps",
        "path": "/multi-night-visit-maps",
        "kind": "dayobs",
        "instrument": True,
    },
    {"name": "static-visit-map", "path": "/static-visit-map", "kind": "dayobs", "instrument": True},
    {"name": "block-details", "path": "/block-details", "kind": "block", "instrument": False},
]

BOKEH_ID_RE = re.compile(r"^(?:p\d+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")
ID_KEYS = {"id", "root_id", "target_id"}

EPOCH_NS_RE = re.compile(r"^\d{19}$")
# Bokeh stamps the render time, in nanoseconds since the epoch, into the
# document. It tracks when the figure was built rather than anything about
# the data, so it differs on every call. Bounded to a plausible date range
# (roughly 2020-2100) so a genuine 19-digit datum is not swallowed.
EPOCH_NS_MIN, EPOCH_NS_MAX = 1_500_000_000_000_000_000, 4_200_000_000_000_000_000
RENDER_TIMESTAMP = "<render-timestamp>"


def add_days(dayobs: int, days: int) -> int:
    date = datetime.strptime(str(dayobs), "%Y%m%d") + timedelta(days=days)
    return int(date.strftime("%Y%m%d"))


def git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def canonicalise(value, state: dict):
    """Replace run-varying values with stable stand-ins.

    ``state`` accumulates the id mapping (so ids are renumbered in
    first-seen order) and the set of transform names that fired, which
    the caller records in the manifest.
    """
    if isinstance(value, dict):
        # The _encode_png_payload shape: swap the payload for a digest.
        mime = value.get("mime_type")
        if isinstance(mime, str) and mime.startswith("image/") and isinstance(value.get("data"), str):
            state["transforms"].add("image-payload-hashed")
            raw = base64.b64decode(value["data"])
            return {
                "mime_type": mime,
                "data_sha256": hashlib.sha256(raw).hexdigest(),
                "data_bytes": len(raw),
            }
        return {key: canonicalise(item, state) for key, item in value.items()}

    if isinstance(value, list):
        return [canonicalise(item, state) for item in value]

    if isinstance(value, str) and BOKEH_ID_RE.match(value):
        state["transforms"].add("bokeh-ids-renumbered")
        return state["ids"].setdefault(value, f"id-{len(state['ids'])}")

    if isinstance(value, str) and EPOCH_NS_RE.match(value) and EPOCH_NS_MIN <= int(value) <= EPOCH_NS_MAX:
        # Masked to one constant rather than numbered: these carry no
        # identity, so position among them is not meaningful either.
        state["transforms"].add("render-timestamps-masked")
        return RENDER_TIMESTAMP

    return value


def canonicalise_response(payload):
    state = {"ids": {}, "transforms": set()}
    # Ids are only meaningful under the keys that carry them; walking the
    # whole payload would rewrite any string that happens to look like one.
    return canonicalise(payload, state), sorted(state["transforms"])


def calls_for(endpoint: dict, args) -> list[tuple[str, dict]]:
    """Every (slug, params) pair for one endpoint."""
    if endpoint["kind"] == "block":
        return [(endpoint["name"], {"key": args.block_keys})]

    day1 = args.day_start
    ranges = [("1day", day1, add_days(day1, 1)), ("7day", day1, add_days(day1, 7))]
    instruments = args.instruments if endpoint["instrument"] else [None]

    calls = []
    for label, start, end in ranges:
        for instrument in instruments:
            params = {"dayObsStart": start, "dayObsEnd": end}
            params.update(endpoint.get("extra", {}))
            slug = f"{endpoint['name']}__{label}"
            if instrument is not None:
                params["instrument"] = instrument
                slug = f"{slug}__{instrument}"
            calls.append((slug, params))
    return calls


def capture(args) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    session = requests.Session()

    manifest = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_commit": git_commit(),
        "base_url": args.base_url,
        "day_start": args.day_start,
        "instruments": args.instruments,
        "block_keys": args.block_keys,
        "files": {},
    }

    failures = 0
    for endpoint in selected_endpoints(args):
        for slug, params in calls_for(endpoint, args):
            print(f"[capture] {slug} ...", flush=True)
            try:
                response = session.get(
                    f"{args.base_url}{endpoint['path']}", params=params, timeout=args.timeout
                )
            except requests.RequestException as exc:
                print(f"[capture] {slug} FAILED: {exc}", flush=True)
                manifest["files"][slug] = {"error": str(exc)}
                failures += 1
                continue

            try:
                payload = response.json()
            except ValueError:
                payload = {"__non_json_body__": response.text}

            body, transforms = canonicalise_response(payload)
            document = {"status": response.status_code, "body": body}
            path = out / f"{slug}.json"
            path.write_text(json.dumps(document, sort_keys=True, indent=2) + "\n")

            manifest["files"][slug] = {
                "status": response.status_code,
                "bytes": len(response.content),
                "transforms": transforms,
            }
            if response.status_code >= 400:
                print(f"[capture] {slug} -> HTTP {response.status_code}", flush=True)

    (out / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    print(f"\n[capture] wrote {len(manifest['files'])} files to {out}", flush=True)
    if failures:
        print(f"[capture] {failures} request(s) failed outright", flush=True)
    return 1 if failures else 0


def selected_endpoints(args):
    if not args.endpoints:
        return ENDPOINTS
    unknown = set(args.endpoints) - {e["name"] for e in ENDPOINTS}
    if unknown:
        sys.exit(f"unknown endpoint(s): {', '.join(sorted(unknown))}")
    return [e for e in ENDPOINTS if e["name"] in args.endpoints]


REORDER_MARK = "reordered:"
"""Prefix marking a difference that is purely a change of list order.

Lets `compare` tell "the same records came back in a different order"
apart from "the records themselves changed", without re-analysing the
message text.
"""


def _canonical(value) -> str:
    """A hashable, order-insensitive identity for one list element.

    Object key order is not meaningful, so sorting keys means two
    equivalent records canonicalise identically.
    """
    return json.dumps(value, sort_keys=True)


def _summarise(counts, limit: int = 3) -> str:
    shown = [f"{item[:80]} x{n}" if n > 1 else item[:80] for item, n in list(counts.items())[:limit]]
    if len(counts) > limit:
        shown.append(f"... {len(counts) - limit} more")
    return "; ".join(shown)


def diff_json(before, after, path: str = "") -> list[str]:
    """Recursive structural diff, deepest-specific path first.

    Lists are checked as multisets before position-by-position, so a
    reordering is reported once rather than as a difference at every
    index. A multiset (rather than a set) keeps duplicates significant:
    ``[a, a, b]`` and ``[a, b, b]`` hold the same distinct values but
    are not the same list.
    """
    if type(before) is not type(after):
        return [f"{path or '<root>'}: type {type(before).__name__} -> {type(after).__name__}"]

    if isinstance(before, dict):
        differences = []
        for key in sorted(set(before) | set(after)):
            child = f"{path}.{key}" if path else key
            if key not in before:
                differences.append(f"{child}: added")
            elif key not in after:
                differences.append(f"{child}: removed")
            else:
                differences.extend(diff_json(before[key], after[key], child))
        return differences

    if isinstance(before, list):
        if before == after:
            return []
        label = path or "<root>"
        before_counts = Counter(_canonical(item) for item in before)
        after_counts = Counter(_canonical(item) for item in after)

        if before_counts == after_counts:
            return [f"{label}: {REORDER_MARK} same {len(before)} elements, different order"]

        removed, added = before_counts - after_counts, after_counts - before_counts
        if len(before) != len(after):
            differences = [f"{label}: length {len(before)} -> {len(after)}"]
            if removed:
                differences.append(f"{label}: {sum(removed.values())} removed — {_summarise(removed)}")
            if added:
                differences.append(f"{label}: {sum(added.values())} added — {_summarise(added)}")
            return differences

        # Same length, different contents: positional detail is the
        # informative view, since indices line up.
        differences = []
        for index, (a, b) in enumerate(zip(before, after)):
            differences.extend(diff_json(a, b, f"{path}[{index}]"))
        return differences

    if before != after:
        return [f"{path or '<root>'}: {before!r} -> {after!r}"]
    return []


def compare(args) -> int:
    before_dir, after_dir = Path(args.before), Path(args.after)
    before_files = {p.name for p in before_dir.glob("*.json")} - {"manifest.json"}
    after_files = {p.name for p in after_dir.glob("*.json")} - {"manifest.json"}

    identical = reordered = differing = 0
    rows, details = [], []

    # Every file gets a row, matched or not: a parity claim is only as good
    # as the evidence that each capture was actually checked.
    for name in sorted(before_files | after_files):
        if name not in after_files:
            rows.append((name, "MISSING from after", ""))
            continue
        if name not in before_files:
            rows.append((name, "MISSING from before", ""))
            continue

        # Canonicalise again on load. It is idempotent, and it means a
        # capture taken before a masking rule existed still compares
        # correctly without being re-taken.
        before, _ = canonicalise_response(json.loads((before_dir / name).read_text()))
        after, _ = canonicalise_response(json.loads((after_dir / name).read_text()))
        differences = diff_json(before, after)

        status = before.get("status")
        # Both sides erroring identically is parity, but not success —
        # surface the code so it cannot pass as a clean match.
        note = "" if status == 200 else f" (HTTP {status})"

        if not differences:
            # Deep equality of the parsed objects, independent of the walk.
            identical += 1
            rows.append((name, f"OK{note}", ""))
            continue

        if all(REORDER_MARK in line for line in differences):
            reordered += 1
            verdict = "ORDER ONLY"
        else:
            differing += 1
            verdict = "DIFFERS"
        rows.append((name, f"{verdict}{note}", str(len(differences))))

        shown = differences[: args.max_lines]
        extra = len(differences) - len(shown)
        details.append((name, shown, extra))

    print("| File | Verdict | Differences |")
    print("|---|---|---|")
    for name, verdict, count in rows:
        print(f"| `{name}` | {verdict} | {count} |")

    for name, shown, extra in details:
        print(f"\n**`{name}`**\n")
        for line in shown:
            print(f"- `{line}`")
        if extra:
            print(f"- ... {extra} more")

    print(f"\n{identical} identical, {reordered} differing only in list order, {differing} differing.")
    if reordered:
        print("\nSame records, different sequence — a real contract change, but not a data change.")

    return 0 if not differing and not reordered and before_files == after_files else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)

    cap = sub.add_parser("capture", help="Call every endpoint and dump the responses")
    cap.add_argument("--out", required=True, help="Directory to write the capture into")
    cap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Backend base URL — uvicorn, not nginx")
    cap.add_argument(
        "--day-start",
        type=int,
        default=DEFAULT_DAY_START,
        help="First dayobs of a fixed historical test week (YYYYMMDD)",
    )
    cap.add_argument("--instruments", nargs="*", default=DEFAULT_INSTRUMENTS)
    cap.add_argument("--block-keys", nargs="*", default=DEFAULT_BLOCK_KEYS)
    cap.add_argument("--endpoints", nargs="*", help="Subset of endpoint names (default: all)")
    cap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)

    cmp_ = sub.add_parser("compare", help="Diff two capture directories")
    cmp_.add_argument("before")
    cmp_.add_argument("after")
    cmp_.add_argument("--max-lines", type=int, default=20, help="Differences shown per file")

    args = parser.parse_args()
    return capture(args) if args.mode == "capture" else compare(args)


if __name__ == "__main__":
    sys.exit(main())
