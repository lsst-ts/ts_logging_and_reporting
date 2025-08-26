import logging
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError, BaseLogrepError
from lsst.ts.logging_and_reporting.utils import get_access_token

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
from .services.context_feed_service import get_context_feed

from .services.rubin_nights_service import get_time_accounting, get_open_close_dome, make_json_safe
from rubin_nights.connections import get_clients


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

def get_rubin_nights_clients(auth_token: str = Depends(get_access_token)):
    return get_clients(auth_token=auth_token)


@app.get("/health")
async def health():
    """Health check endpoint.
    Used by kubernetes readiness and liveness probes.
    """
    return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/mock-exposures")
async def read_exposures_from_mock_data(
    request: Request, dayObsStart: int, dayObsEnd: int, instrument: str
):

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
    clients: dict = Depends(get_rubin_nights_clients),
):
    logger.info(
        f"Getting exposures for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument}"
    )
    try:
        exposures = get_exposures(
            dayObsStart, dayObsEnd, instrument, auth_token=auth_token
        )
        on_sky_exposures = [exp for exp in exposures if exp.get("can_see_sky")]
        total_exposure_time = sum(exposure["exp_time"] for exposure in exposures)
        total_on_sky_exposure_time = sum(exp["exp_time"] for exp in on_sky_exposures)

        open_dome_times = get_open_close_dome(dayObsStart, dayObsEnd, instrument, clients['efd'])
        open_dome_hours = 0
        if not open_dome_times.empty:
            open_dome_hours = open_dome_times['open_hours'].sum()

        exposures_df = get_time_accounting(
            dayObsStart,
            dayObsEnd,
            instrument,
            exposures,
            clients["efd"],
        )

        if not exposures_df.empty:

            exposures_dict = exposures_df[["exposure_id", "exposure_name", "exp_time", "img_type",
                "observation_reason", "science_program", "target_name", "can_see_sky",
                "band", "obs_start", "physical_filter", "day_obs", "seq_num",
                "obs_end", "overhead", "zero_point_median", "visit_id", "overhead",
                "pixel_scale_median", "psf_sigma_median"]].to_dict(orient="records")

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
    logger.info(
        f"Getting data log for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument}"
    )
    try:
        records = get_data_log(
            dayObsStart, dayObsEnd, instrument, auth_token=auth_token
        )
        return jsonable_encoder({"data_log": records})

    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /data-log: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /data-log: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jira-tickets")
async def read_jira_tickets(
    request: Request, dayObsStart: int, dayObsEnd: int, instrument: str
):

    logger.info(
        f"Getting jira tickets for start: "
        f"{dayObsStart}, end: {dayObsEnd} "
        f"and instrument: {instrument}"
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
    logger.info(
        f"Getting almanac for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}"
    )
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
        records = get_messages(
            dayObsStart, dayObsEnd, instrument, auth_token=auth_token
        )
        time_lost_to_weather = sum(
            msg["time_lost"] for msg in records if msg["time_lost_type"] == "weather"
        )
        time_lost_to_faults = sum(
            msg["time_lost"] for msg in records if msg["time_lost_type"] == "fault"
        )
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
        flags = get_exposure_flags(
            dayObsStart, dayObsEnd, instrument, auth_token=auth_token
        )
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
        entries = get_exposurelog_entries(
            dayObsStart, dayObsEnd, instrument, auth_token=auth_token
        )
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
        print(f"CONTEXT FEED read begins.")
        (efd_and_messages, cols) = get_context_feed(dayObsStart, dayObsEnd, auth_token=auth_token)
        # print(f"CONTEXT FEED cols: ", cols)
        # print(f"CONTEXT FEED DATA: ", efd_and_messages)
        print(f"CONTEXT FEED read completed.")

        print(f"TYPE efd_and_messages: ", type(efd_and_messages)) # Now encoded as list?

        #  This is where the error is coming from, from the dataframe/list? ------------------------ <--- ERROR
        return {
            # "data": efd_and_messages,
            "cols": cols,
        }
    except Exception as e:
        logger.error(f"Error in /context-feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
