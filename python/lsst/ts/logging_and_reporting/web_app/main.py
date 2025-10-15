from datetime import datetime, timedelta
import logging
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from bokeh.embed import json_item

from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError, BaseLogrepError
from lsst.ts.logging_and_reporting.utils import get_access_token, make_json_safe

from .services.jira_service import get_jira_tickets
from .services.consdb_service import (
    get_mock_exposures,
    get_exposures,
    get_data_log,
)
from .services.almanac_service import get_almanac
from .services.narrativelog_service import get_messages
from .services.exposurelog_service import get_exposure_flags, get_exposurelog_entries
from .services.nightreport_service import get_night_reports
from .services.rubin_nights_service import get_time_accounting, get_open_close_dome, get_context_feed
from .services.scheduler_service import create_visit_skymaps

from schedview.compute.visits import add_coords_tuple
from schedview.collect.visits import read_visits, NIGHT_STACKERS
from rubin_scheduler.scheduler.model_observatory import ModelObservatory


logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


app = FastAPI()

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


@app.get("/health")
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
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_access_token),
):
    logger.info(f"Getting exposures for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}")
    try:
        exposures = get_exposures(dayObsStart, dayObsEnd, instrument, auth_token=auth_token)
        on_sky_exposures = [exp for exp in exposures if exp.get("can_see_sky")]
        total_exposure_time = sum(exposure["exp_time"] for exposure in exposures)
        total_on_sky_exposure_time = sum(exp["exp_time"] for exp in on_sky_exposures)

        open_dome_times = get_open_close_dome(dayObsStart, dayObsEnd, instrument, auth_token)
        open_dome_hours = 0
        if not open_dome_times.empty:
            open_dome_hours = open_dome_times["open_hours"].sum()

        exposures_df = get_time_accounting(
            dayObsStart,
            dayObsEnd,
            instrument,
            exposures,
            auth_token,
        )

        if not exposures_df.empty:
            exposures_dict = exposures_df[
                [
                    "exposure_id",
                    "exposure_name",
                    "exp_time",
                    "img_type",
                    "observation_reason",
                    "science_program",
                    "target_name",
                    "can_see_sky",
                    "band",
                    "obs_start",
                    "physical_filter",
                    "day_obs",
                    "seq_num",
                    "obs_end",
                    "overhead",
                    "zero_point_median",
                    "visit_id",
                    "overhead",
                    "pixel_scale_median",
                    "psf_sigma_median",
                ]
            ].to_dict(orient="records")

            exposures_safe_dict = make_json_safe(exposures_dict)

            exposures = jsonable_encoder(exposures_safe_dict)

        return {
            "exposures": exposures,
            "exposures_count": len(exposures),
            "sum_exposure_time": total_exposure_time,
            "on_sky_exposures_count": len(on_sky_exposures),
            "total_on_sky_exposure_time": total_on_sky_exposure_time,
            "open_dome_hours": open_dome_hours,
        }

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /exposures: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /exposures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data-log")
async def read_data_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_access_token),
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
async def read_jira_tickets(request: Request, dayObsStart: int, dayObsEnd: int, instrument: str):
    logger.info(
        f"Getting jira tickets for start: {dayObsStart}, end: {dayObsEnd} and instrument: {instrument}"
    )
    try:
        tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument)
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
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
    auth_token: str = Depends(get_access_token),
):
    try:
        (efd_and_messages, cols) = get_context_feed(dayObsStart, dayObsEnd, auth_token=auth_token)
        return {
            "data": efd_and_messages,
            "cols": cols,
        }
    except Exception as e:
        logger.error(f"Error in /context-feed: {e}")


@app.get("/multi-night-visit-maps")
async def multi_night_visit_maps(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    planisphereOnly: bool = False,
    appletMode: bool = False,
    auth_token: str = Depends(get_access_token),
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
    planisphereOnly : `bool`, optional
        If True, generate only the planisphere map. Default is False.
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
        f"planisphereOnly: {planisphereOnly}"
    )
    try:
        observatory = ModelObservatory(init_load_length=1)

        dayobs_start_dt = datetime.strptime(str(dayObsStart), '%Y%m%d')
        dayobs_end_dt = datetime.strptime(str(dayObsEnd), '%Y%m%d')
        diff = dayobs_end_dt - dayobs_start_dt

        visits = read_visits(
            dayobs_end_dt.date() - timedelta(days=1),
            instrument.lower(),
            stackers = NIGHT_STACKERS, num_nights=diff.days)

        visits['filter'] = visits['band']

        v_map = None

        if len(visits):
            visits = add_coords_tuple(visits)

            v_map, _ = create_visit_skymaps(
                visits=visits,
                timezone="UTC",
                observatory=observatory,
                planisphere_only=planisphereOnly,
                applet_mode=appletMode,
                theme="DARK",
            )

        return {
            "interactive": json_item(v_map) if v_map is not None else {},
            }

    except Exception as e:
        logger.error(f"Error in /multi-night-visit-maps: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/survey-progress-map")
async def survey_progress_map(
    request: Request,
    dayObs: int,
    instrument: str,
    auth_token: str = Depends(get_access_token),
):
    """Generate a survey progress map for a given night using Bokeh.

    Parameters
    ----------
    request : `Request`
        FastAPI request object.
    dayObs : `int`
        Date in YYYYMMDD format.
    instrument : `str`
        Instrument name (e.g., 'lsstCam', 'latiss', etc.).
    auth_token : `str`
        Authentication token (injected by FastAPI dependency).

    Returns
    -------
    `dict`
        A dictionary containing the Bokeh JSON item for
        the static survey progress map.
    """
    logger.info(
        f"Getting survey progress map for night: "
        f"{dayObs} and instrument: {instrument}"
    )
    try:
        import time
        import numpy as np
        from schedview.plot.survey import create_metric_visit_map_grid
        from rubin_sim import maf

        observatory = ModelObservatory(init_load_length=1)

        dayobs_dt = datetime.strptime(str(dayObs), '%Y%m%d')

        start_time = time.perf_counter()

        visits = read_visits(
            dayobs_dt.date(),
            instrument.lower(),
            stackers = NIGHT_STACKERS, num_nights=50)

        visits['filter'] = visits['band']

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.debug(f"read_visits() executed in {elapsed_time:.6f} seconds")

        s_map = None

        if len(visits):

            start_time = time.perf_counter()

            dayobs_visits = visits[visits['day_obs'] == dayObs]

            previous_day_obs_dt = dayobs_dt - timedelta(days=1)
            previous_day_obs = previous_day_obs_dt.strftime('%Y%m%d')

            previous_visits = visits[visits['day_obs'] == int(previous_day_obs)]

            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            logger.debug(f"fetching previous night visits executed in {elapsed_time:.6f} seconds")

            if len(dayobs_visits) and len(previous_visits) \
                    and not np.all(np.isnan(dayobs_visits['fiveSigmaDepth'])) \
                    and not np.all(np.isnan(previous_visits['fiveSigmaDepth'])):
                start_time = time.perf_counter()
                s_map = create_metric_visit_map_grid(
                    maf.CountMetric(col='fiveSigmaDepth', metric_name="Numbers of visits"),
                    previous_visits.loc[np.isfinite(previous_visits['fiveSigmaDepth']), :],
                    visits.loc[np.isfinite(visits['fiveSigmaDepth']), :],
                    observatory,
                    nside=32,
                    use_matplotlib=False
                )
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                logger.debug(f"create_metric_visit_map_grid() executed in {elapsed_time:.6f} seconds")

        return {
            "static": json_item(s_map) if s_map is not None  else {}
            }
    except Exception as e:
        logger.error(f"Error in /survey-progress-map: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
