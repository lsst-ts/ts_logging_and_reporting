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
"""FastAPI entrypoint and route definitions"""

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import __version__, services
from .middleware import (
    CacheControlMiddleware,
    DayobsValidationMiddleware,
    RequestLoggingMiddleware,
)
from .redis_client import get_redis_client
from .services.worker_pool_mixin import preload_worker_modules
from .utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the worker pools before serving, and stop them after.

    Starting here rather than on first use means the forkserver has
    imported each service's dependencies by the time requests arrive;
    uvicorn holds off accepting them until this returns.
    """
    pooled = [factory() for factory in services.WORKER_POOL_SERVICES]
    preload_worker_modules(pooled)
    for service in pooled:
        service.start_worker_pool()
    yield
    for service in pooled:
        service.shutdown_worker_pool()


app = FastAPI(
    root_path="/nightlydigest/api",
    docs_url="/docs",
    openapi_url="/openapi.json",
    redoc_url=None,
    lifespan=lifespan,
)

origins = [
    "http://localhost:5173",  # Vite
    "http://127.0.0.1:5173",  # just in case
    "http://nightlydigest-nginx-service",  # Kubernetes service name
]

# add_middleware prepends, so the last added is the outermost:
# RequestLogging -> CacheControl -> CORS -> DayobsValidation -> route.
# CORS has to stay outside DayobsValidation, or its 422 reaches the
# browser without CORS headers and surfaces as a CORS failure instead of
# the validation message. CacheControl is outside CORS so it has the last
# word on Cache-Control, and sees short-circuited responses like that 422.
# RequestLogging is outermost so its timing covers all of the above.
app.add_middleware(DayobsValidationMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CacheControlMiddleware)
app.add_middleware(RequestLoggingMiddleware)


logger.info("Starting FastAPI app")


@app.get("/version")
@app.get("/version/")
async def get_version():
    """Get the current version of the package."""
    return JSONResponse(status_code=200, content={"version": __version__})


@app.get("/health")
@app.get("/health/")
async def health():
    """Health check endpoint.

    Used by kubernetes readiness and liveness probes.
    """
    return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/flush-redis")
@app.get("/flush-redis/")
def flush_redis():
    """Empty the cache database, for performance testing only.

    Temporary: `scripts/perf_test.py --use-endpoint-flush` uses this to
    reach a Redis it has no `redis-cli` access to, such as one inside a
    remote deployment. It is removed before the refactor merges.
    """
    get_redis_client().flushdb()
    logger.warning("Redis cache flushed via /flush-redis")
    return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/expected-exposures")
def read_expected_exposures(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_expected_exposures_service),
):
    """Return the expected (simulated) visit count for the range.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Inclusive upper bound of the requested dayobs range.

    Returns
    -------
    dict
        ``sum_exposures``, the total nominal visit count summed over the
        pre-night simulations for each dayobs in the range.

    Raises
    ------
    fastapi.HTTPException
        404 when no matching simulation exists for a requested night,
        502 if the simulation archive cannot be reached.
    """
    return service.handle_request(dayObsStart, dayObsEnd)


@app.get("/jira-tickets")
def read_jira_tickets(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_jira_tickets_service),
):
    """Return OBS Jira tickets created or updated during the range.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used to select tickets by their system field.

    Returns
    -------
    dict
        ``issues``, the matching tickets with their summary, systems and
        Jira URL.

    Raises
    ------
    fastapi.HTTPException
        500 if the Jira hostname is not configured, 502 if the Jira
        query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get("/narrative-log")
def read_narrative_log(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_narrative_log_service),
):
    """Return narrative log messages and time lost for the range.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used to select messages by telescope.

    Returns
    -------
    dict
        ``narrative_log`` with the messages, plus
        ``time_lost_to_weather`` and ``time_lost_to_faults`` summed over
        them.

    Raises
    ------
    fastapi.HTTPException
        502 if the narrative log query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get("/exposure-flags")
def read_exposure_flags(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_exposure_flags_service),
):
    """Return the exposure flag set on each flagged exposure.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used to select exposure log messages.

    Returns
    -------
    dict
        ``exposure_flags``, one ``obs_id``/``exposure_flag`` pair per
        flagged exposure. Exposures with no flag are omitted.

    Raises
    ------
    fastapi.HTTPException
        502 if the exposure log query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get("/exposure-entries")
def read_exposure_entries(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_exposure_entries_service),
):
    """Return exposure log entries for the range.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used to select exposure log messages.

    Returns
    -------
    dict
        ``exposure_entries``, the messages for the range ordered by the
        time they were added.

    Raises
    ------
    fastapi.HTTPException
        502 if the exposure log query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get("/night-reports")
def read_night_reports(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_night_report_service),
):
    """Return night report records for the range.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.

    Returns
    -------
    dict
        ``reports``, the night reports for the range ordered by dayobs.

    Raises
    ------
    fastapi.HTTPException
        502 if the night report query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd)


@app.get("/block-details")
def read_block_details(
    keys: list[str] = Query(..., alias="key"),
    service=Depends(services.get_block_details_service),
):
    """Retrieve BLOCK details from Zephyr/Jira for a list of keys.

    Parameters
    ----------
    key : list[str]
        List of BLOCK keys (e.g. ``BLOCK-704`` or ``BLOCK-T123_a``)
        provided as repeated query parameters. ``BLOCK-Tnnn`` resolves
        against Zephyr Scale and ``BLOCK-nnn`` against Jira.

    Returns
    -------
    dict
        A ``data`` field mapping each resolved BLOCK key to its
        summary, URL, and source, and an ``errors`` field mapping each
        failed source (``zephyr``, ``jira``) to its error message.
        Keys that resolve in neither source are omitted.

        A failure of one source alone is not an error status: the
        response is a 200 carrying the surviving source's results in
        ``data`` and the failed source's message in ``errors``, so its
        keys are missing from ``data`` for the same reason unresolved
        keys are. Callers that distinguish the two must check
        ``errors`` before reading a missing key as unresolved.

    Raises
    ------
    fastapi.HTTPException
        500 if both the Zephyr and Jira requests fail, or if the Jira
        hostname is not configured.
    """
    return service.handle_request(keys)
