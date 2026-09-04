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


@app.get("/context-feed")
def read_context_feed(
    dayObsStart: int,
    dayObsEnd: int,
    service=Depends(services.get_context_feed_service),
):
    """Return the consolidated ScriptQueue context feed for the range.

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
        502 if the context feed query fails.
    """
    return service.handle_request(dayObsStart, dayObsEnd)
