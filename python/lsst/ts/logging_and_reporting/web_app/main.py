import datetime
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from services.jira_service import get_jira_tickets
from services.consdb_service import get_mock_exposures, custom_get_exposures
from services.almanac_service import get_almanac
from services.narrativelog_service import get_messages


logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


app = FastAPI()

origins = [
    "http://localhost:5173",  # Vite
    "http://127.0.0.1:5173",  # just in case
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Change to your React app origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


logger.info("Starting FastAPI app")


@app.get("/mock-exposures")
async def read_exposures_from_mock_data(
    request: Request,
    dayObsStart: datetime.date,
    dayObsEnd: datetime.date,
    instrument: str
    ):
    logger.info("Getting exposures from mock data")
    exposures = get_mock_exposures(dayObsStart, dayObsEnd, instrument)
    return {"exposures": exposures}


@app.get("/exposures")
async def test_read_exposures(
    request: Request,
    dayObsStart: datetime.date,
    dayObsEnd: datetime.date,
    instrument: str):
    logger.info(f"Getting exposures for start: "
                f"{dayObsStart}, end: {dayObsEnd} "
                f"and instrument: {instrument}")
    exposures = custom_get_exposures(dayObsStart, dayObsEnd, instrument)
    total_exposure_time = sum(exposure["exp_time"] for exposure in exposures)
    return {
        "exposures_count": len(exposures),
        "sum_exposure_time": total_exposure_time}


@app.get("/jira-tickets")
async def read_jira_tickets(
    request: Request,
    dayObsStart:datetime.date,
    dayObsEnd: datetime.date,
    instrument: str):
    logger.info(f"Getting jira tickets res from mock data for start: "
                f"{dayObsStart}, end: {dayObsEnd} "
                f"and instrument: {instrument}")
    tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument)
    return {"issues": tickets}


@app.get("/almanac")
async def read_almanac(request: Request, dayObsStart: datetime.date, dayObsEnd: datetime.date):
    logger.info(f"Getting alamanc for dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    almanac = get_almanac(dayObsStart, dayObsEnd)
    return {"night_hours": almanac.night_hours}


@app.get("/narrative-log")
async def read_narrative_log(
    request: Request,
    dayObsStart: datetime.date,
    dayObsEnd: datetime.date,
    instrument: str):
    logger.info(f"Getting Narrative Log records for dayObsStart: {dayObsStart}, "
                f"dayObsEnd: {dayObsEnd} and instrument: {instrument}")
    records = get_messages(dayObsStart, dayObsEnd, "LSSTComCam")
    time_lost_to_weather = sum(msg["time_lost"] for msg in records if msg["time_lost_type"] == 'weather')
    return {"narrative_log": records, "time_lost_to_weather": time_lost_to_weather}
