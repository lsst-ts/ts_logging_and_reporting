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

from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL, MUTABLE_TTL, TODAY_TTL
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

logger = logging.getLogger(__name__)

# Endpoints whose data is mutable regardless of which dayobs is
# requested.
_MUTABLE_PATHS = frozenset(
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

    - `TODAY_TTL` if the response includes today's astronomical
      dayobs, so proxy and browser caches never serve data more stale
      than one ``RefreshWorker`` cycle.
    - `MUTABLE_TTL` for historical requests to the mutable endpoints
      (``/exposure-flags``, ``/block-details``, ``/exposure-entries``,
      ``/narrative-log``), whose data can change on any dayobs.
    - `HISTORIC_TTL` for other fully historical requests, whose data
      will not change.

    Endpoints without dayobs parameters (``/version``, ``/health``)
    are left untouched, except mutable endpoints, which get
    `MUTABLE_TTL` (``/block-details`` is keyed by BLOCK id, not
    dayobs).

    Query parameter names handled:

    - ``dayObs`` — single-day endpoints
    - ``dayObsStart`` + ``dayObsEnd`` — range endpoints
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        if response.status_code >= 400:
            response.headers["Cache-Control"] = "no-store"
            return response

        path = request.url.path.rstrip("/")
        mutable = path in _MUTABLE_PATHS

        params = request.query_params

        day_obs_raw = params.get("dayObs")
        start_raw = params.get("dayObsStart")
        end_raw = params.get("dayObsEnd")

        # Single dayObs implies a one-day range
        if day_obs_raw and not (start_raw or end_raw):
            start_raw = day_obs_raw
            end_raw = day_obs_raw

        if not (start_raw or end_raw):
            if mutable:
                response.headers["Cache-Control"] = f"public, max-age={MUTABLE_TTL}"
            return response

        try:
            start = int(start_raw) if start_raw else None
            end = int(end_raw) if end_raw else None
        except (ValueError, TypeError):
            logger.warning(f"Uncacheable non-integer dayobs range for {path}: {start_raw}..{end_raw}")
            return response

        # If only one bound is present, treat it as both
        start = start if start is not None else end
        end = end if end is not None else start

        if start <= current_dayobs() <= end:
            max_age = TODAY_TTL
        elif mutable:
            max_age = MUTABLE_TTL
        else:
            max_age = HISTORIC_TTL

        response.headers["Cache-Control"] = f"public, max-age={max_age}"
        return response
