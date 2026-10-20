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
from pydantic import BaseModel

from . import __version__, services
from .middleware import (
    CacheControlMiddleware,
    DayobsValidationMiddleware,
    RequestLoggingMiddleware,
)
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

# Swagger (/docs) renders a route's docstring as Markdown, which mangles
# numpydoc sections. FastAPI leaves everything after a form feed (\f) out
# of the OpenAPI description, so each route's docstring has its summary
# above a \f for Swagger, and its numpydoc below it for developers.
# Swagger's parameter table takes its descriptions from
# Query(description=...), and its responses table takes error codes from
# the route's responses=, so keep both in step with the numpydoc.


INTERNAL_ERROR = "An unexpected internal error, such as the cache being unreachable."


class ErrorResponse(BaseModel):
    """Body of an error response raised as an HTTPException."""

    detail: str


def error_responses(descriptions: dict[int, str]) -> dict[int | str, dict[str, Any]]:
    """Build a route's ``responses`` entries for its error codes.

    Parameters
    ----------
    descriptions : dict[int, str]
        Description of each error status code the route can return.

    Returns
    -------
    dict[int | str, dict[str, Any]]
        One entry per status code, each with an ``ErrorResponse`` body.
    """
    return {
        code: {"model": ErrorResponse, "description": description}
        for code, description in descriptions.items()
    }


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


@app.get(
    "/exposures",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The ConsDB exposure query failed."}),
)
def read_exposures(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name used for the ConsDB exposure query."),
    service=Depends(services.get_exposures_service),
) -> dict[str, Any]:
    """Return exposures and derived night-summary metrics.

    \f
    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used for the ConsDB exposure query.

    Returns
    -------
    dict[str, Any]
        JSON-serializable response containing exposure records and
        totals, dome-open summaries, and twilight-windowed
        time-accounting metrics.

    Raises
    ------
    fastapi.HTTPException
        422 for an unrecognised instrument or malformed dayobs, 502 if
        the ConsDB exposure query fails. Dome-open and time-accounting
        sub-query failures are reported in the response payload instead.
        500 for an unexpected internal error, such as the cache being
        unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get(
    "/expected-exposures",
    responses=error_responses(
        {
            404: "No matching simulation exists for a requested night.",
            500: INTERNAL_ERROR,
            502: "The simulation archive could not be reached.",
        }
    ),
)
def read_expected_exposures(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Inclusive upper bound of the requested dayobs range."),
    service=Depends(services.get_expected_exposures_service),
):
    """Return the expected (simulated) visit count for the range.

    \f
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
        502 if the simulation archive cannot be reached, 500 for an
        unexpected internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd)


@app.get(
    "/data-log",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The ConsDB query failed."}),
)
def read_data_log(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name used for the ConsDB exposure query."),
    service=Depends(services.get_data_log_service),
):
    """Return the full ConsDB exposure record for each exposure.

    \f
    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used for the ConsDB exposure query.

    Returns
    -------
    dict
        ``data_log``, the exposure records for the range, JSON-safe.

    Raises
    ------
    fastapi.HTTPException
        422 for an unrecognised instrument or malformed dayobs, 502 if
        the ConsDB query fails, 500 for an unexpected internal error,
        such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get(
    "/jira-tickets",
    responses=error_responses(
        {
            500: "The Jira hostname is not configured, or an unexpected internal error occurred.",
            502: "The Jira query failed.",
        }
    ),
)
def read_jira_tickets(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name used to select tickets by their system field."),
    service=Depends(services.get_jira_tickets_service),
):
    """Return OBS Jira tickets created or updated during the range.

    \f
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
        500 if the Jira hostname is not configured or for an unexpected
        internal error, such as the cache being unreachable, 502 if the
        Jira query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get("/almanac", responses=error_responses({500: INTERNAL_ERROR}))
def read_almanac(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    service=Depends(services.get_almanac_service),
):
    """Return almanac records for the nights in the range.

    \f
    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.

    Returns
    -------
    dict
        ``almanac_info``, one record per night with sun and moon
        rise/set times, twilight boundaries and elapsed twilight hours.

    Raises
    ------
    fastapi.HTTPException
        500 for an unexpected internal error, such as the cache being
        unreachable.

    Notes
    -----
    Almanac records are labeled by the dayobs of their morning twilight
    boundary, so the range yields records labeled ``dayObsStart + 1``
    through ``dayObsEnd``.
    """
    return service.handle_request(dayObsStart, dayObsEnd)


@app.get(
    "/narrative-log",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The narrative log query failed."}),
)
def read_narrative_log(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name used to select messages by telescope."),
    service=Depends(services.get_narrative_log_service),
):
    """Return narrative log messages and time lost for the range.

    \f
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
        502 if the narrative log query fails, 500 for an unexpected
        internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get(
    "/exposure-flags",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The exposure log query failed."}),
)
def read_exposure_flags(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name used to select exposure log messages."),
    service=Depends(services.get_exposure_flags_service),
):
    """Return the exposure flag set on each flagged exposure.

    \f
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
        502 if the exposure log query fails, 500 for an unexpected
        internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get(
    "/exposure-entries",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The exposure log query failed."}),
)
def read_exposure_entries(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name used to select exposure log messages."),
    service=Depends(services.get_exposure_entries_service),
):
    """Return exposure log entries for the range.

    \f
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
        502 if the exposure log query fails, 500 for an unexpected
        internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)


@app.get(
    "/night-reports",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The night report query failed."}),
)
def read_night_reports(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    service=Depends(services.get_night_report_service),
):
    """Return night report records for the range.

    \f
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
        502 if the night report query fails, 500 for an unexpected
        internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd)


@app.get(
    "/context-feed",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The context feed query failed."}),
)
def read_context_feed(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Inclusive upper bound of the requested dayobs range."),
    service=Depends(services.get_context_feed_service),
):
    """Return the consolidated ScriptQueue context feed for the range.

    \f
    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Inclusive upper bound of the requested dayobs range.

    Returns
    -------
    dict
        ``data``, the feed records in time order, and ``cols``, the
        column order the frontend renders them in.

    Raises
    ------
    fastapi.HTTPException
        502 if the context feed query fails, 500 for an unexpected
        internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd)


@app.get(
    "/obs-status",
    responses=error_responses({500: INTERNAL_ERROR, 502: "The status event or almanac query failed."}),
)
def read_obs_status(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Inclusive upper bound of the requested dayobs range."),
    includeEntries: bool = Query(True, description="If True, include the raw status events in the response."),
    includeIntervals: bool = Query(
        False,
        description="If True, include the derived status intervals in the response.",
    ),
    nightOnlyMetrics: bool = Query(
        True,
        description="If False, time outside night hours contributes to the metrics.",
    ),
    metrics: list[str] | None = Query(
        None,
        alias="metric",
        description=(
            "Metrics to compute, given as repeated `metric` parameters. "
            "Unrecognised names are logged and skipped."
        ),
    ),
    service=Depends(services.get_obs_status_service),
):
    """Return observatory status events, intervals and derived metrics.

    \f
    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Inclusive upper bound of the requested dayobs range.
    includeEntries : bool, optional
        If True, include the raw status events in the response.
    includeIntervals : bool, optional
        If True, include the derived status intervals in the response.
    nightOnlyMetrics : bool, optional
        If False, time outside night hours contributes to the metrics.
    metrics : list[str] or None, optional
        Metrics to compute, given as repeated ``metric`` parameters.
        Unrecognised names are logged and skipped.

    Returns
    -------
    dict
        Any of ``entries``, ``intervals`` and ``metrics`` according to
        the flags above, plus ``availability`` describing how much of
        the requested range predates the availability of obs-status
        data.

    Raises
    ------
    fastapi.HTTPException
        502 if the status event or almanac query fails, 500 for an
        unexpected internal error, such as the cache being unreachable.
    """
    return service.handle_request(
        dayObsStart,
        dayObsEnd,
        includeEntries,
        includeIntervals,
        nightOnlyMetrics,
        metrics,
    )


@app.get(
    "/multi-night-visit-maps",
    responses=error_responses(
        {
            500: INTERNAL_ERROR,
            502: "The ConsDB exposure query failed.",
            503: "The map worker pool is busy; try again shortly.",
            504: "Generating the map timed out.",
        }
    ),
)
def read_multi_night_visit_maps(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name, such as `lsstCam` or `latiss`."),
    appletMode: bool = Query(False, description="If True, generate maps suitable for applet display."),
    service=Depends(services.get_visit_maps_service),
):
    """Generate multi-night visit maps using Bokeh.

    \f
    Parameters
    ----------
    dayObsStart : `int`
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : `int`
        Exclusive upper bound of the requested dayobs range.
    instrument : `str`
        Instrument name, such as ``lsstCam`` or ``latiss``.
    appletMode : `bool`, optional
        If True, generate maps suitable for applet display. Default is False.

    Returns
    -------
    `dict`
        A dictionary containing the Bokeh JSON item for the interactive map.

    Raises
    ------
    fastapi.HTTPException
        502 if the ConsDB exposure query fails, 503 when the map worker
        pool is busy, 504 when generating the map times out, 500 for an
        unexpected internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument, appletMode)


@app.get(
    "/block-details",
    responses=error_responses(
        {
            500: (
                "Both the Zephyr and Jira requests failed, the Jira hostname is not "
                "configured, or an unexpected internal error occurred."
            )
        }
    ),
)
def read_block_details(
    keys: list[str] = Query(
        ...,
        alias="key",
        description=(
            "BLOCK keys (e.g. `BLOCK-704` or `BLOCK-T123_a`), given as "
            "repeated `key` parameters. `BLOCK-Tnnn` resolves against "
            "Zephyr Scale and `BLOCK-nnn` against Jira."
        ),
    ),
    service=Depends(services.get_block_details_service),
):
    """Retrieve BLOCK details from Zephyr/Jira for a list of keys.

    \f
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
        500 if both the Zephyr and Jira requests fail, if the Jira
        hostname is not configured, or for an unexpected internal
        error, such as the cache being unreachable.
    """
    return service.handle_request(keys)


@app.get(
    "/static-visit-map",
    responses=error_responses(
        {
            500: INTERNAL_ERROR,
            502: "The ConsDB exposure query failed.",
            503: "The map worker pool is busy; try again shortly.",
            504: "Generating the map timed out.",
        }
    ),
)
def read_static_visit_map(
    dayObsStart: int = Query(description="Inclusive lower bound of the requested dayobs range."),
    dayObsEnd: int = Query(description="Exclusive upper bound of the requested dayobs range."),
    instrument: str = Query(description="Instrument name, such as `lsstCam` or `latiss`."),
    service=Depends(services.get_static_visit_map_service),
):
    """Generate a static visit map for a date range and instrument.

    \f
    Parameters
    ----------
    dayObsStart : `int`
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : `int`
        Exclusive upper bound of the requested dayobs range.
    instrument : `str`
        Instrument name, such as ``lsstCam`` or ``latiss``.

    Returns
    -------
    result : `dict`
        Dictionary containing the base64-encoded PNG image for the static
        visit map.

    Raises
    ------
    fastapi.HTTPException
        502 if the ConsDB exposure query fails, 503 when the map worker
        pool is busy, 504 when generating the map times out, 500 for an
        unexpected internal error, such as the cache being unreachable.
    """
    return service.handle_request(dayObsStart, dayObsEnd, instrument)
