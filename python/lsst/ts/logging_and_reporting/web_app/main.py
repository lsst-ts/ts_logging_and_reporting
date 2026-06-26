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
import re
from typing import Any, List

import pandas as pd
from bokeh.embed import json_item
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from lsst.ts.logging_and_reporting.exceptions import BaseLogrepError, ConsdbQueryError
from lsst.ts.logging_and_reporting.utils import (
    build_block_response,
    get_access_token,
    get_jira_hostname,
    make_json_safe,
)

from .. import __version__
from .services.almanac_service import get_almanac
from .services.consdb_service import (
    get_data_log,
    get_exposures,
    get_mock_exposures,
)
from .services.exposurelog_service import get_exposure_flags, get_exposurelog_entries
from .services.jira_service import get_block_ticket_summaries, get_jira_tickets
from .services.narrativelog_service import get_messages
from .services.nightreport_service import get_night_reports
from .services.rubin_nights_service import (
    _compute_closed_hours,
    _current_dayobs_utc,
    get_context_feed,
    get_obs_status,
    get_open_close_dome,
    get_time_accounting,
    get_visits,
)
from .services.scheduler_service import (
    build_static_visit_map,
    build_visit_maps_using_builder,
    get_expected_exposures,
)
from .services.zephyr_service import get_test_cases

# Auth dependencies (instantiated once for reuse and testing)
rsp_auth = get_access_token()
jira_auth = get_access_token("jira")
zephyr_auth = get_access_token("zephyr")

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


app = FastAPI(root_path="/nightlydigest/api", docs_url="/docs", openapi_url="/openapi.json", redoc_url=None)

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
        Authentication token injected from the request context.

    Returns
    -------
    dict[str, Any]
        JSON-serialisable response containing raw exposure records,
        exposure totals, dome-open summaries, and twilight-windowed
        time-accounting metrics.

    Raises
    ------
    fastapi.HTTPException
        Raised with status 502 if the underlying ConsDB query fails, or
        500 for any other unhandled error. Dome-open and time-accounting
        sub-query failures are reported in the response payload instead.
    """
    logger.info(f"Getting exposures for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    try:
        exposures = get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        on_sky_exposures = [exp for exp in exposures if exp.get("can_see_sky")]
        total_exposure_time = sum(exp.get("exp_time") or 0 for exp in exposures)
        total_on_sky_exposure_time = sum(exp.get("exp_time") or 0 for exp in on_sky_exposures)

        open_dome_times_records, open_dome_hours_records, open_dome_error = [], {}, None
        try:
            open_dome_times = get_open_close_dome(dayObsStart, dayObsEnd, instrument, auth_token)

            if open_dome_times is not None and not open_dome_times.empty:
                open_dome_times_records = make_json_safe(open_dome_times.to_dict(orient="records"))

                try:
                    open_dome_totals = open_dome_times.groupby("day_obs").agg(
                        {"night_hours": "max", "open_hours": "sum", "sunset12": "first", "sunrise12": "first"}
                    )
                    now_utc = pd.Timestamp.now(tz="UTC")
                    current_dayobs = _current_dayobs_utc(now_utc)
                    open_dome_totals["closed_hours"] = open_dome_totals.apply(
                        lambda row: _compute_closed_hours(row, current_dayobs, now_utc),
                        axis=1,
                    )
                    open_dome_hours_records = make_json_safe(open_dome_totals.to_dict(orient="index"))
                except Exception as e:
                    logger.error(f"Error aggregating open/close dome times: {e}", exc_info=True)
                    open_dome_hours_records = None
                    open_dome_error = "Failed to aggregate dome open hours"

        except Exception as e:
            logger.error(
                f"Error getting open/close dome times from rubin_nights through EFD: {e}", exc_info=True
            )
            open_dome_times_records = None
            open_dome_hours_records = None
            open_dome_error = "Failed to retrieve dome open/close times"

        night_time_on_sky_sums, time_accounting_error = None, None
        try:
            night_time_on_sky_sums = get_time_accounting(
                dayObsStart, dayObsEnd, instrument, exposures, auth_token
            )
        except Exception as e:
            logger.error(f"Error computing time accounting in /exposures: {e}", exc_info=True)
            time_accounting_error = "Failed to compute night time accounting"

        return {
            "exposures": exposures,
            "exposures_count": len(exposures),
            "sum_exposure_time": total_exposure_time,
            "on_sky_exposures_count": len(on_sky_exposures),
            "total_on_sky_exposure_time": total_on_sky_exposure_time,
            "open_dome_times": open_dome_times_records,
            "day_obs_open_dome_hours": open_dome_hours_records,
            "open_dome_error": open_dome_error,
            "night_on_sky_time_accounting": night_time_on_sky_sums,
            "time_accounting_error": time_accounting_error,
        }

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /exposures: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /exposures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/expected-exposures")
async def read_expected_exposures(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
):
    logger.info(f"Getting expected exposures for start: {dayObsStart}, end: {dayObsEnd} ")
    try:
        expected_exposures = get_expected_exposures(
            dayObsStart,
            dayObsEnd,
        )

        return {"sum_exposures": expected_exposures["sum"]}

    except Exception as e:
        logger.error(f"Error in /expected-exposures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data-log")
async def read_data_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(f"Getting data log for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    try:
        records = get_data_log(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        return jsonable_encoder({"data_log": records})

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /data-log: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /data-log: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jira-tickets")
async def read_jira_tickets(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(jira_auth),
    jira_hostname: str = Depends(get_jira_hostname),
):
    logger.info(
        f"Getting jira tickets for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument, auth_token, jira_hostname)
        return {"issues": tickets}
    except BaseLogrepError as ble:
        logger.error(f"Jira API error in /jira-tickets: {ble}")
        raise HTTPException(status_code=502, detail="Jira API query failed")
    except Exception as e:
        logger.error(f"Error in /jira-tickets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/almanac")
async def read_almanac(request: Request, dayObsStart: int, dayObsEnd: int):
    logger.info(f"Getting almanac for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    try:
        almanac_info = get_almanac(dayObsStart, dayObsEnd)
        return {"almanac_info": almanac_info}
    except Exception as e:
        logger.error(f"Error in /almanac: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/narrative-log")
async def read_narrative_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Getting Narrative Log records for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        records = get_messages(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        time_lost_to_weather = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == "weather")
        time_lost_to_faults = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == "fault")
        return {
            "narrative_log": records,
            "time_lost_to_weather": time_lost_to_weather,
            "time_lost_to_faults": time_lost_to_faults,
        }
    except Exception as e:
        logger.error(f"Error in /narrative-log: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/exposure-flags")
async def read_exposure_flags(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Getting Exposure Log flags for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        flags = get_exposure_flags(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        return {
            "exposure_flags": flags,
        }
    except Exception as e:
        logger.error(f"Error in /exposure-flags: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/exposure-entries")
async def read_exposure_entries(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(rsp_auth),
):
    logger.info(
        f"Getting Exposure Log entries for dayObsStart: {dayObsStart}, "
        f"dayObsEnd: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        entries = get_exposurelog_entries(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        return {
            "exposure_entries": entries,
        }
    except Exception as e:
        logger.error(f"Error in /exposure-entries: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/night-reports")
async def read_nightreport(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    auth_token: str = Depends(rsp_auth),
):
    try:
        records = get_night_reports(dayObsStart, dayObsEnd, auth_token=auth_token)
        return {
            "reports": records,
        }
    except Exception as e:
        logger.error(f"Error in /night-reports: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
async def read_block_details(
    request: Request,
    keys: List[str] = Query(..., alias="key"),
    zephyr_token: str = Depends(zephyr_auth),
    jira_token: str = Depends(jira_auth),
    jira_hostname: str = Depends(get_jira_hostname),
):
    """Retrieve BLOCK details from Zephyr/Jira for a list of keys.

    Parameters
    ----------
    request : `fastapi.Request`
        FastAPI request object.
    key : `list` [`str`]
        List of BLOCK keys (e.g., ``BLOCK-704`` or
        ``BLOCK-T123_a``) provided as query parameters.
    zephyr_token : `str`
        Authentication token for Zephyr (injected by FastAPI dependency).
    jira_token : `str`
        Authentication token for Jira (injected by FastAPI dependency).
    jira_hostname : `str`
        Authentication hostname for Jira (injected by FastAPI dependency).

    Returns
    -------
    `dict`
        A dictionary containing a ``data`` field that maps each valid BLOCK key
        to its associated summary, URL, and source (e.g., Zephyr or Jira).

    Raises
    ------
    HTTPException
        Raised with status code 500 if an unexpected error occurs while
        retrieving BLOCK details.
    """
    logger.info(f"Getting BLOCK details from Zephyr/Jira for: {keys}")
    try:
        ZEPHYR_BLOCK_RE = re.compile(r"^BLOCK-T\d+(?:_[A-Za-z0-9]+)?$")
        JIRA_BLOCK_RE = re.compile(r"^BLOCK-\d+$")

        zephyr_keys = []
        jira_keys = []

        # Remove duplicates
        key = list(dict.fromkeys(keys))

        # Sort keys by data source
        for k in key:
            if ZEPHYR_BLOCK_RE.match(k):
                zephyr_keys.append(k)
            elif JIRA_BLOCK_RE.match(k):
                jira_keys.append(k)

        zephyr_blocks = {}
        jira_blocks = {}

        errors = {}

        # Get BLOCK descriptions from Zephyr
        if zephyr_keys:
            try:
                logger.info(f"Getting Test Case BLOCK details from Zephyr for {zephyr_keys}")
                zephyr_blocks = await get_test_cases(
                    zephyr_keys,
                    zephyr_token=zephyr_token,
                    jira_token=jira_token,
                )
            except Exception as e:
                logger.error(f"Zephyr error in /block-details: {e}", exc_info=True)
                errors["zephyr"] = str(e)

        # Get Test Case BLOCK descriptions from Jira
        if jira_keys:
            try:
                logger.info(f"Getting BLOCK ticket summaries from Jira for {jira_keys}")
                jira_blocks = get_block_ticket_summaries(
                    jira_keys,
                    jira_token=jira_token,
                    jira_hostname=jira_hostname,
                )
            except Exception as e:
                logger.error(f"Jira error in /block-details: {e}", exc_info=True)
                errors["jira"] = str(e)

        # If both failed → hard fail
        if "zephyr" in errors and "jira" in errors:
            raise HTTPException(status_code=500, detail="Both Zephyr and Jira requests failed.")

        # Flesh out response dict with source type and URL
        data = build_block_response(zephyr_blocks, jira_blocks)

        return {
            "data": data,
            "errors": errors,
        }

    # Catch double service failures
    except HTTPException:
        raise

    # Catch other errors
    except Exception as e:
        logger.error(f"Error in /block-details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
