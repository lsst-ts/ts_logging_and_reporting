import datetime
from typing import Union
import httpx

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse, JSONResponse

from services.jira import get_jira_tickets
from services.consdb_service import get_exposures
from services.almanac_service import get_almanac


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


@app.get("/")
async def read_root(request: Request):
    print(request.headers)
    print(request.cookies)
    response = RedirectResponse(
        "https://usdf-rsp.slac.stanford.edu/login?rd=https://usdf-rsp.slac.stanford.edu/auth/api/v1/token-info",
    )
    # response.headers['Access-Control-Allow-Origin'] = request.headers['Origin']
    print(response)
    return response


@app.get("/exposures")
async def read_exposures(request: Request, dayObsStart: datetime.date, dayObsEnd: datetime.date, instrument: str):
    exposures = get_exposures(dayObsStart, dayObsEnd, instrument)
    return {"exposures": exposures}


@app.get("/jira-tickets")
async def read_jira_tickets(request: Request, dayObsStart: datetime.date, dayObsEnd: datetime.date, instrument: str):
    tickets = get_jira_tickets(dayObsStart, dayObsEnd, instrument)
    return {"issues": tickets}
    

@app.get("/almanac")
async def read_almanac(request: Request, dayObsStart: datetime.date, dayObsEnd: datetime.date):
    print(f"hellloooo: dayObsStart: {dayObsStart}, dayObsEnd: {dayObsEnd}")
    almanac = get_almanac(dayObsStart, dayObsEnd)
    print(almanac)
    return {"almanac": almanac}


# @app.get("/token")
# def create_token():
#     return "my-token"


# @app.get("/test")
# async def read_root(request: Request):
#     print(request.cookies.get("gafaelfawr"))
#     auth = ("user", "my-token")
#     response = httpx.get(
#         # "https://usdf-rsp.slac.stanford.edu/auth/api/v1/token-info",
#         "https://usdf-rsp-dev.slac.stanford.edu/exposurelog/exposures?registry=1&instrument=LSSTComCam&min_day_obs=20241205&max_day_obs=20241209&limit=10",
#         auth=auth
#     )

#     # if response.status_code == 200:
#     print(response)
#     return response


# @app.get("/items/{item_id}")
# def read_item(item_id: int, q: Union[str, None] = None):
#     return {"item_id": item_id, "q": q}


# @app.get("/user-info")
# def read_user_info(request: Request):
#     refresh_token = "my-token"
#     response = JSONResponse({"user_info":{
#             "username": "eali",
#             "email": "ee@gmail.com"
#         }})
#     response.set_cookie(
#         key="nd_token",
#         value=refresh_token,
#         # httponly=True,
#         secure=False,  # use True in production with HTTPS
#         samesite="lax",
#         # max_age=60 * 60 * 24 * 7,
#         # path="/token"
#     )
#     return response
