#!/usr/bin/env python3
"""
Generate static files for the Nightly Digest backend.

Calls the running backend via HTTP and saves each response as a static file,
named after the endpoint + query string. Designed to be run periodically
(e.g., every 5 minutes) to keep static files up to date.

See STATIC_GENERATOR_PLAN.md for full design documentation.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package is required. Install with: pip install requests")
    sys.exit(1)


logger = logging.getLogger(__name__)

INSTRUMENTS = ["LATISS", "LSSTCam"]

OBS_STATUS_DIGEST_METRICS = ["fault_loss", "weather"]
OBS_STATUS_CONTEXT_FEED_METRICS = [
    "daytime",
    "operational",
    "fault",
    "weather",
    "downtime",
    "idle",
    "unknown",
]


# ---------------------------------------------------------------------------
# DayObs helpers
# ---------------------------------------------------------------------------


def get_current_dayobs() -> int:
    """Return today's dayobs integer (YYYYMMDD) based on noon-UTC boundary.

    DayObs runs from noon UTC to noon UTC. Before noon UTC, the current
    dayobs is yesterday's calendar date; at or after noon it is today's.
    """
    now = datetime.now(timezone.utc)
    if now.hour < 12:
        d = now.date() - timedelta(days=1)
    else:
        d = now.date()
    return int(d.strftime("%Y%m%d"))


def dayobs_to_date(dayobs: int) -> date:
    return datetime.strptime(str(dayobs), "%Y%m%d").date()


def dayobs_add_days(dayobs: int, days: int) -> int:
    """Add (or subtract) days from a dayobs integer."""
    d = dayobs_to_date(dayobs) + timedelta(days=days)
    return int(d.strftime("%Y%m%d"))


def generate_dayobs_combinations(today: int, max_days_ago: int) -> list:
    """Return all contiguous (start, end) dayobs pairs.

    Generates every sub-range of [today - max_days_ago, today],
    ordered largest span to smallest.
    """
    combos = []
    # span = number of days between start and end (0 = single day)
    for span in range(max_days_ago, -1, -1):
        # slide the window: start_offset goes from -max_days_ago up to -span
        for start_offset in range(-max_days_ago, -span + 1):
            end_offset = start_offset + span
            start = dayobs_add_days(today, start_offset)
            end = dayobs_add_days(today, end_offset)
            combos.append((start, end))
    return combos


# ---------------------------------------------------------------------------
# URL / filename building
# ---------------------------------------------------------------------------


def _build_query_string(params: dict) -> str:
    """Build a query string from a params dict, supporting repeated keys.

    Values that are lists produce repeated key=value pairs.
    Boolean values are lowercased strings (true/false).
    """
    parts = []
    for key, value in params.items():
        if isinstance(value, list):
            for v in value:
                parts.append(f"{key}={v}")
        elif isinstance(value, bool):
            parts.append(f"{key}={'true' if value else 'false'}")
        else:
            parts.append(f"{key}={value}")
    return "&".join(parts)


def build_filename(endpoint: str, params: dict) -> str:
    """Build the static filename for an endpoint + params dict.

    The filename is the endpoint name (without leading slash) plus the
    canonical query string, e.g.:
        exposures?dayObsStart=20260625&dayObsEnd=20260625&instrument=LSSTCam
    """
    if not params:
        return endpoint
    return f"{endpoint}?{_build_query_string(params)}"


def build_url(backend_url: str, endpoint: str, params: dict) -> str:
    """Build the full backend URL for a request."""
    base = backend_url.rstrip("/")
    if not params:
        return f"{base}/{endpoint}"
    return f"{base}/{endpoint}?{_build_query_string(params)}"


# ---------------------------------------------------------------------------
# Regeneration logic
# ---------------------------------------------------------------------------


def should_regenerate(
    file_path: str,
    start: int,
    end: int,
    today: int,
    yesterday: int,
    is_new_day: bool,
    force_refresh: bool,
    historic_mode: bool,
) -> bool:
    """Decide whether to fetch and write this file.

    Rules (in priority order):
    1. --force-refresh: always regenerate.
    2. File doesn't exist: always create.
    3. Historic mode (--dayobs-override w/o --force-refresh): skip.
    4. Normal mode: overwrite if file spans today, or spans
       yesterday on the first run of a new day.
    """
    if force_refresh:
        return True

    if not os.path.exists(file_path):
        return True

    if historic_mode:
        return False

    # Normal mode: refresh files that reference today's (still-changing) data
    if start <= today <= end:
        return True

    # On first run of a new day, refresh yesterday's files (may have late data)
    if is_new_day and (start <= yesterday <= end):
        return True

    return False


def should_regenerate_block_details(
    file_path: str,
    touches: dict,
    is_new_day: bool,
    force_refresh: bool,
    historic_mode: bool,
) -> bool:
    """Decide whether to fetch and write a block-details file.

    block-details filenames contain no dates, so regeneration is driven by
    whether the key set was seen in any combo spanning today or yesterday,
    rather than by the filename itself.

    Rules mirror should_regenerate:
    1. --force-refresh: always regenerate.
    2. File doesn't exist: always create.
    3. Historic mode: never overwrite.
    4. Normal mode: regenerate if key set spans today (data still changing),
       or spans yesterday on the first run of a new day.
    """
    if force_refresh:
        return True

    if not os.path.exists(file_path):
        return True

    if historic_mode:
        return False

    if touches["today"]:
        return True

    if is_new_day and touches["yesterday"]:
        return True

    return False


# ---------------------------------------------------------------------------
# Fetch and save
# ---------------------------------------------------------------------------


def fetch_and_save(url: str, file_path: str, timeout: int):
    """Fetch url and atomically save the response body to file_path.

    Returns a (start_ts, end_ts, success) tuple. Uses a temp file +
    os.rename() for atomic writes. Retries up to 2 times with backoff
    on connection errors.
    """
    tmp_path = file_path + ".tmp"
    max_retries = 2
    start_ts = time.time()

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            end_ts = time.time()
            logger.warning("HTTP error (%.2fs): %s", end_ts - start_ts, e)
            return (start_ts, end_ts, False)
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries:
                wait = 2**attempt
                logger.warning(
                    "Connection error (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1,
                    max_retries + 1,
                    wait,
                    e,
                )
                time.sleep(wait)
                continue
            end_ts = time.time()
            logger.warning(
                "Connection error after %d attempts (%.2fs): %s",
                max_retries + 1,
                end_ts - start_ts,
                e,
            )
            return (start_ts, end_ts, False)
        except requests.exceptions.Timeout:
            end_ts = time.time()
            logger.warning(
                "Request timed out after %ds (%.2fs)",
                timeout,
                end_ts - start_ts,
            )
            return (start_ts, end_ts, False)

        # Write atomically
        try:
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            os.rename(tmp_path, file_path)
            end_ts = time.time()
            return (start_ts, end_ts, True)
        except OSError as e:
            # Clean up temp file if possible
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            end_ts = time.time()
            logger.error("File write error (%.2fs): %s", end_ts - start_ts, e)
            raise  # Abort on write errors — likely a permissions problem


# ---------------------------------------------------------------------------
# Block-details key extraction
# ---------------------------------------------------------------------------


def extract_block_keys_from_data_log(file_path: str) -> set:
    """Read a saved data-log file and extract unique science_program values."""
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        records = data.get("data_log", [])
        return {r["science_program"] for r in records if r.get("science_program")}
    except (OSError, json.JSONDecodeError, KeyError):
        return set()


def extract_block_keys_from_exposures(file_path: str) -> set:
    """Read a saved exposures file and extract science_program values."""
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        records = data.get("exposures", [])
        return {r["science_program"] for r in records if r.get("science_program")}
    except (OSError, json.JSONDecodeError, KeyError):
        return set()


def extract_block_keys_from_context_feed(file_path: str) -> set:
    """Read a saved context-feed file and extract BLOCK keys.

    category_index == 10 is "AUTOLOG (Simonyi)" — automated log entries from
    the Simonyi telescope. BLOCK execution events are recorded here with the
    BLOCK identifier (e.g. BLOCK-123) in the name field.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        records = data.get("records", [])
        # records is a list of lists; need column names for indices
        columns = data.get("columns", [])
        if not columns or not records:
            return set()
        try:
            cat_idx = columns.index("category_index")
            name_idx = columns.index("name")
        except ValueError:
            return set()
        return {
            row[name_idx]
            for row in records
            if row[cat_idx] == 10 and row[name_idx] and row[name_idx].startswith("BLOCK")
        }
    except (OSError, json.JSONDecodeError, KeyError, IndexError):
        return set()


# ---------------------------------------------------------------------------
# Progress counter helper
# ---------------------------------------------------------------------------


class Progress:
    def __init__(self, total: int):
        self.total = total
        self.current = 0
        self.created = 0
        self.skipped = 0
        self.errors = 0

    def tick(self, status: str, filename: str, detail: str = ""):
        self.current += 1
        suffix = f" ({detail})" if detail else ""
        logger.info("[%d/%d] %s %s%s", self.current, self.total, status, filename, suffix)

    def record_created(self, filename: str, dry_run: bool, start: float = None, end: float = None):
        self.created += 1
        timing = ""
        if start is not None and end is not None:
            start_str = datetime.fromtimestamp(start, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            end_str = datetime.fromtimestamp(end, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            duration = end - start
            timing = f"{duration:.2f}s | {start_str} -> {end_str}"
        elif start is not None and end is None:
            start_str = datetime.fromtimestamp(start, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            timing = f" [{start_str}]"
        self.tick("Would create" if dry_run else "Created", filename, timing)

    def record_skipped(self, filename: str, reason: str = "exists"):
        self.skipped += 1
        self.tick("Skipped", filename, reason)

    def record_error(self, filename: str, start: float, end: float):
        self.errors += 1
        start_str = datetime.fromtimestamp(start, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        end_str = datetime.fromtimestamp(end, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        timing = f"{end - start:.2f}s | {start_str} -> {end_str}"
        self.tick("ERROR", filename, timing)

    def summary(self, batch_start: float = None):
        logger.info(
            "\nSummary: %d created, %d skipped, %d errors",
            self.created,
            self.skipped,
            self.errors,
        )
        if batch_start is not None:
            batch_end = time.time()
            batch_duration = batch_end - batch_start
            end_utc = datetime.fromtimestamp(batch_end, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            logger.info("Batch ended at  %s UTC", end_utc)
            logger.info("Total duration: %.1fs", batch_duration)


# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------


def build_dayobs_tasks(
    combos: list,
    today: int,
    yesterday: int,
    is_new_day: bool,
    force_refresh: bool,
    historic_mode: bool,
    output_dir: str,
) -> tuple:
    """Build the full list of tasks for Groups A-C and count skipped files.

    Returns (tasks, skipped_count) where tasks is a list of dicts
    with keys: filename, endpoint, params, start, end — only for
    files needing regeneration.
    """
    tasks = []
    skipped = 0

    def _check(endpoint, params, start, end):
        nonlocal skipped
        filename = build_filename(endpoint, params)
        file_path = os.path.join(output_dir, filename)
        if should_regenerate(
            file_path,
            start,
            end,
            today,
            yesterday,
            is_new_day,
            force_refresh,
            historic_mode,
        ):
            tasks.append(
                {
                    "filename": filename,
                    "endpoint": endpoint,
                    "params": params,
                    "start": start,
                    "end": end,
                }
            )
        elif os.path.exists(file_path):
            skipped += 1

    for start, end in combos:
        base_params = {"dayObsStart": start, "dayObsEnd": end}

        # Group B: dayobs-only endpoints
        for endpoint in ["expected-exposures", "almanac", "night-reports", "context-feed"]:
            _check(endpoint, base_params, start, end)

        # Group C: obs-status variant 1 (Digest)
        _check(
            "obs-status",
            {
                "dayObsStart": start,
                "dayObsEnd": end,
                "includeEntries": True,
                "includeIntervals": True,
                "nightOnlyMetrics": True,
                "metric": OBS_STATUS_DIGEST_METRICS,
            },
            start,
            end,
        )

        # Group C: obs-status variant 2 (ContextFeed)
        _check(
            "obs-status",
            {
                "dayObsStart": start,
                "dayObsEnd": end,
                "includeEntries": True,
                "includeIntervals": False,
                "nightOnlyMetrics": False,
                "metric": OBS_STATUS_CONTEXT_FEED_METRICS,
            },
            start,
            end,
        )

        # Group A: dayobs + instrument endpoints
        for instrument in INSTRUMENTS:
            inst_params = {"dayObsStart": start, "dayObsEnd": end, "instrument": instrument}

            for endpoint in [
                "exposures",
                "data-log",
                "jira-tickets",
                "narrative-log",
                "exposure-flags",
                "exposure-entries",
                "static-visit-map",
            ]:
                _check(endpoint, inst_params, start, end)

            # multi-night-visit-maps has an extra appletMode param
            _check(
                "multi-night-visit-maps",
                {
                    "dayObsStart": start,
                    "dayObsEnd": end,
                    "instrument": instrument,
                    "appletMode": False,
                },
                start,
                end,
            )

    return tasks, skipped


def build_block_details_tasks(
    combos: list,
    output_dir: str,
    today: int,
    yesterday: int,
    is_new_day: bool,
    force_refresh: bool,
    historic_mode: bool,
    dry_run: bool = False,
) -> tuple:
    """Derive block-details tasks from saved files.

    Reads data-log, exposure, and context-feed files. For each unique
    set of BLOCK keys across all combos, produce one task
    (deduplicated by key set). Returns (tasks, skipped_count).
    """
    # Each source (context-feed, per-instrument data-log,
    # per-instrument exposures) contributes its key set independently
    # so generated filenames match each frontend page's requests.
    #
    # keyset_touches maps frozenset -> date-adjacency flags.
    # frozenset is used because sets are unhashable; this
    # deduplicates identical key sets across date combos.
    keyset_touches: dict = {}  # frozenset[str] -> {"today": bool, "yesterday": bool}

    def _record_keyset(frozen: frozenset, start: int, end: int) -> None:
        if frozen not in keyset_touches:
            keyset_touches[frozen] = {"today": False, "yesterday": False}
        if start <= today <= end:
            keyset_touches[frozen]["today"] = True
        if start <= yesterday <= end:
            keyset_touches[frozen]["yesterday"] = True

    for start, end in combos:
        # Context-feed keys (independent of instrument)
        cf_filename = build_filename("context-feed", {"dayObsStart": start, "dayObsEnd": end})
        cf_path = os.path.join(output_dir, cf_filename)
        if os.path.exists(cf_path):
            cf_keys = extract_block_keys_from_context_feed(cf_path)
            if cf_keys:
                _record_keyset(frozenset(cf_keys), start, end)
        elif not dry_run:
            logger.warning(
                "block-details: context-feed missing for %d-%d, skipping key extraction",
                start,
                end,
            )

        # Data-log and exposures keys per instrument
        for instrument in INSTRUMENTS:
            dl_filename = build_filename(
                "data-log",
                {"dayObsStart": start, "dayObsEnd": end, "instrument": instrument},
            )
            dl_path = os.path.join(output_dir, dl_filename)
            if os.path.exists(dl_path):
                dl_keys = extract_block_keys_from_data_log(dl_path)
                if dl_keys:
                    _record_keyset(frozenset(dl_keys), start, end)
            elif not dry_run:
                logger.warning("block-details: data-log missing for %d-%d %s", start, end, instrument)

            exp_filename = build_filename(
                "exposures",
                {"dayObsStart": start, "dayObsEnd": end, "instrument": instrument},
            )
            exp_path = os.path.join(output_dir, exp_filename)
            if os.path.exists(exp_path):
                exp_keys = extract_block_keys_from_exposures(exp_path)
                if exp_keys:
                    _record_keyset(frozenset(exp_keys), start, end)
            elif not dry_run:
                logger.warning("block-details: exposures missing for %d-%d %s", start, end, instrument)

    tasks = []
    skipped = 0

    for frozen_keys, touches in keyset_touches.items():
        sorted_keys = sorted(frozen_keys)
        params = {"key": sorted_keys}
        filename = build_filename("block-details", params)
        file_path = os.path.join(output_dir, filename)

        if should_regenerate_block_details(file_path, touches, is_new_day, force_refresh, historic_mode):
            tasks.append({"filename": filename, "endpoint": "block-details", "params": params})
        elif os.path.exists(file_path):
            skipped += 1

    return tasks, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Generate static files by calling the Nightly Digest backend.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backend-url",
        default="http://localhost:8080/nightlydigest/api",
        help="Base URL of the running backend",
    )
    parser.add_argument(
        "--output-dir",
        default="./static_output",
        help="Directory to write static files into",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        metavar="SECONDS",
        help="Seconds to sleep between HTTP requests",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=7,
        help="How many days back to generate (generates all sub-ranges within the window)",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=120,
        metavar="SECONDS",
        help="HTTP request timeout in seconds (visit maps can be slow)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making requests or writing files",
    )
    parser.add_argument(
        "--dayobs-override",
        type=int,
        metavar="YYYYMMDD",
        help=(
            "Override 'today' with a specific dayobs for historic generation. "
            "Without --force-refresh, only creates missing files and never overwrites."
        ),
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Regenerate all files in the window, overwriting any that already exist",
    )
    parser.add_argument(
        "--block-details-only",
        action="store_true",
        help="Only generate block-details files (skip version and dayobs endpoints)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    args = parser.parse_args()

    level = getattr(logging, args.log_level)
    fmt = logging.Formatter("%(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.addFilter(lambda r: r.levelno < logging.WARNING)
    stdout_handler.setFormatter(fmt)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(max(level, logging.WARNING))
    stderr_handler.setFormatter(fmt)

    logging.basicConfig(level=level, handlers=[stdout_handler, stderr_handler])

    # PID lock to prevent concurrent runs (O_CREAT|O_EXCL is atomic)
    lock_path = "/tmp/generate_static_files.lock"
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode())
        os.close(lock_fd)
    except FileExistsError:
        logger.error("Lock file found: %s. Another instance may be running. Aborting.", lock_path)
        logger.error("Remove %s if you're sure no other instance is running.", lock_path)
        sys.exit(1)

    try:
        # Determine "today"
        if args.dayobs_override is not None:
            today = args.dayobs_override
            historic_mode = True
            logger.info("Historic mode: using dayobs %d as 'today'", today)
        else:
            today = get_current_dayobs()
            historic_mode = False
            logger.info("Today's dayobs: %d", today)

        yesterday = dayobs_add_days(today, -1)

        # Detect first run of new day (only meaningful in normal mode)
        today_sentinel = os.path.join(
            args.output_dir,
            build_filename("almanac", {"dayObsStart": today, "dayObsEnd": today}),
        )
        is_new_day = not historic_mode and not os.path.exists(today_sentinel)
        if is_new_day:
            logger.info(
                "First run for dayobs %d — yesterday's files (%d) will be refreshed",
                today,
                yesterday,
            )

        if args.force_refresh:
            logger.info("Force refresh enabled — all files will be regenerated")

        if args.dry_run:
            logger.info("Dry run mode — no requests will be made and no files will be written")

        os.makedirs(args.output_dir, exist_ok=True)

        batch_start = time.time()
        start_utc = datetime.fromtimestamp(batch_start, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("Batch started at %s UTC", start_utc)

        # Generate all dayobs combinations
        combos = generate_dayobs_combinations(today, args.max_days)

        if not args.block_details_only:
            # --- Pass 1: Group D — version (no dayobs) ---
            version_filename = "version"
            version_file_path = os.path.join(args.output_dir, version_filename)

            # version: only regenerated if missing or force-refresh
            version_tasks = []
            if args.force_refresh or not os.path.exists(version_file_path):
                version_tasks = [{"filename": version_filename, "endpoint": "version", "params": {}}]

            # --- Pass 2: Groups A, B, C — dayobs endpoints ---
            dayobs_tasks, skipped_count = build_dayobs_tasks(
                combos,
                today,
                yesterday,
                is_new_day,
                args.force_refresh,
                historic_mode,
                args.output_dir,
            )

            # version skipped count
            version_skipped = 1 if (not version_tasks and os.path.exists(version_file_path)) else 0

            # Total updated after pass 3 planning
            progress = Progress(len(version_tasks) + len(dayobs_tasks))
            progress.skipped = skipped_count + version_skipped

            # --- Pass 1 ---
            for task in version_tasks:
                logger.info("\nVersion fetch")
                url = build_url(args.backend_url, task["endpoint"], task["params"])
                file_path = os.path.join(args.output_dir, task["filename"])
                if args.dry_run:
                    progress.record_created(task["filename"], dry_run=True)
                else:
                    start, end, ok = fetch_and_save(url, file_path, args.request_timeout)
                    if ok:
                        progress.record_created(task["filename"], dry_run=False, start=start, end=end)
                    else:
                        progress.record_error(task["filename"], start=start, end=end)
                if args.rate_limit > 0 and not args.dry_run:
                    time.sleep(args.rate_limit)

            # --- Pass 2 ---
            logger.info("\nStarting dayobs %d fetches (%d files)", today, len(dayobs_tasks))
            for task in dayobs_tasks:
                url = build_url(args.backend_url, task["endpoint"], task["params"])
                file_path = os.path.join(args.output_dir, task["filename"])
                if args.dry_run:
                    progress.record_created(task["filename"], dry_run=True)
                else:
                    start, end, ok = fetch_and_save(url, file_path, args.request_timeout)
                    if ok:
                        progress.record_created(task["filename"], dry_run=False, start=start, end=end)
                    else:
                        progress.record_error(task["filename"], start=start, end=end)
                if args.rate_limit > 0 and not args.dry_run:
                    time.sleep(args.rate_limit)
        else:
            # Block-only mode: initialize progress with 0 tasks
            progress = Progress(0)

        # --- Pass 3: Group E — block-details (derived) ---
        block_tasks, block_skipped = build_block_details_tasks(
            combos,
            args.output_dir,
            today,
            yesterday,
            is_new_day,
            args.force_refresh,
            historic_mode,
            dry_run=args.dry_run,
        )

        progress.total += len(block_tasks)
        progress.skipped += block_skipped

        logger.info("\nStarting block-details fetches (%d files)", len(block_tasks))
        for task in block_tasks:
            url = build_url(args.backend_url, task["endpoint"], task["params"])
            file_path = os.path.join(args.output_dir, task["filename"])
            if args.dry_run:
                progress.record_created(task["filename"], dry_run=True)
            else:
                start, end, ok = fetch_and_save(url, file_path, args.request_timeout)
                if ok:
                    progress.record_created(task["filename"], dry_run=False, start=start, end=end)
                else:
                    progress.record_error(task["filename"], start=start, end=end)
            if args.rate_limit > 0 and not args.dry_run:
                time.sleep(args.rate_limit)

        progress.summary(batch_start)

    finally:
        try:
            os.unlink(lock_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
