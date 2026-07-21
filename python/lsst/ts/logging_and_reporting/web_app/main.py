#
# This file is part of ts_logging_and_reporting.
#
# Developed for Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import base64
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, List

from bokeh.embed import json_item
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from lsst.ts.logging_and_reporting import adapters
from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError
from lsst.ts.logging_and_reporting.utils import get_access_token

from .. import __version__
from . import services
from .middleware import CacheControlMiddleware
from .redis_client import get_redis_client
from .refresh_worker import RefreshWorker
from .services.consdb_service import get_mock_exposures
from .services.rubin_nights_service import (
    get_context_feed,
    get_obs_status,
    get_visits,
)
from .services.scheduler_service import (
    build_static_visit_map,
    build_visit_maps_using_builder,
)

# Auth dependencies (instantiated once for reuse and testing)
rsp_auth = get_access_token()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

refresh_worker = RefreshWorker(
    [
        adapters.get_consdb_exposures_adapter(),
        adapters.get_consdb_visits_adapter(),
        adapters.get_expected_exposures_adapter(),
        adapters.get_exposurelog_adapter(),
        adapters.get_jira_obs_adapter(),
        adapters.get_narrativelog_adapter(),
        adapters.get_nightreport_adapter(),
    ],
    get_redis_client(),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    refresh_worker.start()
    yield
    refresh_worker.stop()


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

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Change to your React app origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CacheControlMiddleware)


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


@app.get("/mock-exposures")
async def read_exposures_from_mock_data(request: Request, dayObsStart: int, dayObsEnd: int, instrument: str):
    logger.info("Getting exposures from mock data")
    exposures = get_mock_exposures(dayObsStart, dayObsEnd, instrument)
    return {"exposures": exposures}


@app.get("/exposures")
async def read_exposures(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
    service=Depends(services.get_exposures_service),
) -> dict[str, Any]:
    """Return exposures and derived night-summary metrics.

    Parameters
    ----------
    dayObsStart : int
        Inclusive lower bound of the requested dayobs range.
    dayObsEnd : int
        Exclusive upper bound of the requested dayobs range.
    instrument : str
        Instrument name used for the ConsDB exposure query.
    auth_token : str, optional
        Authentication token injected from the request context, used for
        the dome and time-accounting sub-queries.

    Returns
    -------
    dict[str, Any]
        JSON-serialisable response containing exposure records and
        totals, dome-open summaries, and twilight-windowed
        time-accounting metrics.

    Raises
    ------
    fastapi.HTTPException
        422 for an unrecognised instrument or malformed dayobs, 502 if
        the ConsDB exposure query fails. Dome-open and time-accounting
        sub-query failures are reported in the response payload instead.
    """
    logger.info(f"Getting exposures for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    return service.handle(dayObsStart, dayObsEnd, instrument, auth_token)


@app.get("/expected-exposures")
async def read_expected_exposures(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_expected_exposures_service),
):
    logger.info(f"Getting expected exposures for start: {dayObsStart}, end: {dayObsEnd} ")
    return service.handle(dayObsStart, dayObsEnd)


@app.get("/data-log")
async def read_data_log(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_data_log_service),
):
    logger.info(f"Getting data log for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    return service.handle(dayObsStart, dayObsEnd, instrument)


@app.get("/jira-tickets")
def read_jira_tickets(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_jira_tickets_service),
):
    logger.info(
        f"Getting jira tickets for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}"
    )
    return service.handle(dayObsStart, dayObsEnd, instrument)


@app.get("/almanac")
def read_almanac(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_almanac_service),
):
    logger.info(f"Getting almanac for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    return service.handle(dayObsStart, dayObsEnd)


@app.get("/narrative-log")
def read_narrative_log(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_narrative_log_service),
):
    logger.info(
        f"Getting Narrative Log records for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    return service.handle(dayObsStart, dayObsEnd, instrument)


@app.get("/exposure-flags")
def read_exposure_flags(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_exposure_flags_service),
):
    logger.info(
        f"Getting Exposure Log flags for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    return service.handle(dayObsStart, dayObsEnd, instrument)


@app.get("/exposure-entries")
def read_exposure_entries(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    service=Depends(services.get_exposure_entries_service),
):
    logger.info(
        f"Getting Exposure Log entries for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    return service.handle(dayObsStart, dayObsEnd, instrument)


@app.get("/night-reports")
def read_nightreport(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_night_report_service),
):
    logger.info(f"Getting Night Report records for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    return service.handle(dayObsStart, dayObsEnd)


@app.get("/context-feed")
async def read_context_feed(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    auth_token: str = Depends(rsp_auth),
):
    try:
        (efd_and_messages, cols) = get_context_feed(dayObsStart, dayObsEnd, auth_token=auth_token)
        return {
            "data": efd_and_messages,
            "cols": cols,
        }
    except Exception as e:
        logger.error(f"Error in /context-feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/obs-status")
async def read_obs_status(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    includeEntries: bool = True,
    includeIntervals: bool = False,
    nightOnlyMetrics: bool = True,
    metrics: list[str] | None = Query(
        None,
        alias="metric",
    ),
    auth_token: str = Depends(rsp_auth),
):
    try:
        return get_obs_status(
            dayObsStart,
            dayObsEnd,
            includeEntries,
            includeIntervals,
            nightOnlyMetrics,
            metrics,
            auth_token=auth_token,
        )

    except Exception as e:
        logger.error(f"Error in /obs-status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _build_multi_night_visit_map(
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    appletMode: bool,
    auth_token: str,
):
    """Run blocking map generation logic outside the event loop."""
    visits = get_visits(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
    v_map = None

    if len(visits):
        v_map = build_visit_maps_using_builder(
            visits=visits,
            applet_mode=appletMode,
        )

    return v_map


@app.get("/multi-night-visit-maps")
async def multi_night_visit_maps(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    appletMode: bool = False,
    auth_token: str = Depends(rsp_auth),
):
    """Generate multi-night visit maps using Bokeh.
    Parameters
    ----------
    request : `Request`
        FastAPI request object.
    dayObsStart : `int`
        Start date in YYYYMMDD format.
    dayObsEnd : `int`
        End date in YYYYMMDD format.
    instrument : `str`
        Instrument name (e.g., 'lsstCam', 'latiss', etc.).
    appletMode : `bool`, optional
        If True, generate maps suitable for applet display. Default is False.
    auth_token : `str`
        Authentication token (injected by FastAPI dependency).

    Returns
    -------
    `dict`
        A dictionary containing the Bokeh JSON item for the interactive map.
    """
    logger.info(
        f"Getting multi night visit maps for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument} in appletMode: {appletMode}, "
    )

    try:
        v_map = await run_in_threadpool(
            _build_multi_night_visit_map,
            dayObsStart,
            dayObsEnd,
            instrument,
            appletMode,
            auth_token,
        )
        return {
            "interactive": json_item(v_map) if v_map is not None else None,
        }

    except Exception as e:
        logger.error(f"Error in /multi-night-visit-maps: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/block-details")
def read_block_details(
    keys: List[str] = Query(..., alias="key"),
    service=Depends(services.get_block_details_service),
):
    logger.info(f"Getting BLOCK details from Zephyr/Jira for: {keys}")
    return service.handle(keys)


def _encode_png_payload(image_bytes):
    """Serialize PNG bytes into a JSON-safe payload.

    Parameters
    ----------
    image_bytes : `bytes`
        Raw bytes of the PNG image.

    Returns
    -------
    `dict` or `None`
        A dictionary containing the MIME type and base64-encoded
        data of the image, or `None` if the input is `None`.
    """
    if image_bytes is None:
        return None

    return {
        "mime_type": "image/png",
        "data": base64.b64encode(image_bytes).decode("ascii"),
    }


@app.get("/static-visit-map")
async def static_visit_map(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    """Generate a static visit map for a date range and instrument.

    Parameters
    ----------
    request : `Request`
        FastAPI request object.
    dayObsStart : `int`
        Start date in YYYYMMDD format.
    dayObsEnd : `int`
        End date in YYYYMMDD format.
    instrument : `str`
        Instrument name, such as ``lsstCam`` or ``latiss``.
    auth_token : `str`
        Authentication token injected by the FastAPI dependency.

    Returns
    -------
    result : `dict`
        Dictionary containing the base64-encoded PNG image for the static
        visit map.
    """
    logger.info(
        "Generating static map for visits between %s and %s (instrument: %s)",
        dayObsStart,
        dayObsEnd,
        instrument,
    )
    try:
        visits = get_visits(dayObsStart, dayObsEnd, instrument, auth_token=auth_token, augment=False)

        png_bytes = build_static_visit_map(visits)

        return {"static_map": _encode_png_payload(png_bytes) if png_bytes else None}

    except ConsdbQueryError as ce:
        logger.error("ConsdbQueryError in /static-map: %s", ce, exc_info=True)
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error("Failed to build visit map: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate static visit map")
