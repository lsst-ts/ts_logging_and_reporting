import logging

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from lsst.ts.logging_and_reporting.exceptions import ConsdbQueryError, BaseLogrepError

from .services.jira_service import get_jira_tickets
from .services.consdb_service import get_mock_exposures, get_exposures
from .services.almanac_service import get_almanac
from .services.narrativelog_service import get_messages
from .services.exposurelog_service import get_exposure_flags

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

def get_auth_token(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or " " not in auth_header:
        raise HTTPException(status_code=401, detail="Authorization header missing or invalid")
    return auth_header.split(" ")[1]


logger.info("Starting FastAPI app")


@app.get("/health")
async def health():
    """Health check endpoint.
    Used by kubernetes readiness and liveness probes.
    """
    return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/mock-exposures")
async def read_exposures_from_mock_data(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str
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
    auth_token: str = Depends(get_auth_token),
):
    logger.info(f"Getting exposures for start: "
                    f"{dayObsStart}, end: {dayObsEnd} "
                    f"and instrument: {instrument}")
    try:
        exposures = get_exposures(dayObsStart, dayObsEnd, instrument, auth_token)
        total_exposure_time = sum(exposure["exp_time"] for exposure in exposures)
        return {
            "exposures": exposures,
            "exposures_count": len(exposures),
            "sum_exposure_time": total_exposure_time
        }
    except ConsdbQueryError as ce:
        logger.error(f"ConsdbQueryError in /exposures: {ce}")
        raise HTTPException(status_code=502, detail="ConsDB query failed")
    except Exception as e:
        logger.error(f"Error in /exposures: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jira-tickets")
async def read_jira_tickets(
    request: Request,
    dayObsStart:int,
    dayObsEnd: int,
    instrument: str
):

    logger.info(f"Getting jira tickets for start: "
                    f"{dayObsStart}, end: {dayObsEnd} "
                    f"and instrument: {instrument}")
    try:
        tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument)
        return {"issues": tickets}
    except BaseLogrepError as ble:
        logger.error(f"Jira API error in /jira-tickets: {ble}")
        raise HTTPException(status_code=502, detail="Jira API query failed")
    except Exception as e:
        logger.error(f"Error in /jira-tickets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/almanac")
async def read_almanac(request: Request, dayObsStart: int, dayObsEnd: int):
    logger.info(f"Getting alamanc for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    try:
        almanac = get_almanac(dayObsStart, dayObsEnd)
        return {"night_hours": almanac.night_hours}
    except Exception as e:
        logger.error(f"Error in /almanac: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/narrative-log")
async def read_narrative_log(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_auth_token),
):
    logger.info(f"Getting Narrative Log records for dayObsStart: {dayObsStart}, "
                f"dayObsEnd: {dayObsEnd} and instrument: {instrument}")
    try:
        records = get_messages(dayObsStart, dayObsEnd, "LSSTComCam", auth_token)
        time_lost_to_weather = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == 'weather')
        time_lost_to_faults = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == 'fault')
        return {
            "narrative_log": records,
            "time_lost_to_weather": time_lost_to_weather,
            "time_lost_to_faults": time_lost_to_faults
        }
    except Exception as e:
        logger.error(f"Error in /narrative-log: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/exposure-flags")
async def read_exposure_flags(
    request: Request,
    dayObsStart: int,
    dayObsEnd: int,
    instrument: str,
    auth_token: str = Depends(get_auth_token),
):
    logger.info(f"Getting Exposure Log flags for dayObsStart: {dayObsStart}, "
                f"dayObsEnd: {dayObsEnd} and instrument: {instrument}")
    try:
        flags = get_exposure_flags(dayObsStart, dayObsEnd, instrument, auth_token)
        return {
            "exposure_flags": flags,
        }
    except Exception as e:
        logger.error(f"Error in /exposure-flags: {e}")
        raise HTTPException(status_code=500, detail=str(e))
