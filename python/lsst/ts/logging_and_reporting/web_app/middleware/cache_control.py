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


import logging

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


from lsst.ts.logging_and_reporting.utils import current_dayobs

logger = logging.getLogger("uvicorn.error")

# Short TTL for responses that include today
# Matches RefreshWorker interval - see doc/BACKEND_REFACTOR_PLAN.md
_TODAY_MAX_AGE = 300
# Long TTL for fully historical responses
_HISTORICAL_MAX_AGE = 86400

# Endpoints that always receive the short TTL because they contain mutable
# data regardless of which dayobs is requested.
_ALWAYS_SHORT_PATHS = frozenset(
    {
        "/exposure-flags",
        "/block-details",
        "/exposure-entries",
        "/narrative-log",
        "/jira-tickets",
    }
)


class CacheControlMiddleware(BaseHTTPMiddleware):
    """Set ``Cache-Control`` headers based on the requested dayobs range.

    Endpoints that accept dayobs query parameters receive a
    ``Cache-Control: max-age=<N>`` header.  The value of ``max-age`` is:

    - ``300`` (short) if the response includes today's astronomical dayobs,
      so proxy and browser caches never serve data more stale than one
      ``RefreshWorker`` cycle.
    - ``86400`` (long) for fully historical requests whose data will not
      change.

    Endpoints without dayobs parameters (``/version``, ``/health``)
    are left untouched.

    Some endpoints (see _ALWAYS_SHORT_PATHS) always receive the short
    TTL because they contain mutable data regardless of dayobs.

    Query parameter names handled:

    - ``dayObs`` — single-day endpoints (e.g. ``/survey-progress-map``)
    - ``dayObsStart`` + ``dayObsEnd`` — range endpoints (all others)
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        path = request.url.path.rstrip("/")

        # Mutable-data endpoints always get the short TTL.
        if path in _ALWAYS_SHORT_PATHS:
            response.headers["Cache-Control"] = f"public, max-age={_TODAY_MAX_AGE}"
            return response

        params = request.query_params

        day_obs_raw = params.get("dayObs")
        start_raw = params.get("dayObsStart")
        end_raw = params.get("dayObsEnd")

        # Single dayObs implies a one-day range
        if day_obs_raw and not (start_raw or end_raw):
            start_raw = day_obs_raw
            end_raw = day_obs_raw

        if not (start_raw or end_raw):
            return response

        try:
            start = int(start_raw) if start_raw else None
            end = int(end_raw) if end_raw else None
        except (ValueError, TypeError):
            logger.error("There's an error getting start/end dayobs")
            return response

        # If only one bound is present, treat it as both
        start = start if start is not None else end
        end = end if end is not None else start

        today = current_dayobs()
        max_age = _TODAY_MAX_AGE if start <= today <= end else _HISTORICAL_MAX_AGE

        response.headers["Cache-Control"] = f"public, max-age={max_age}"
        return response
