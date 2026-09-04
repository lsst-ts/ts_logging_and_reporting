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

import logging

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Match

from lsst.ts.logging_and_reporting.utils.dayobs import dayobs_int_to_date

logger = logging.getLogger(__name__)


def _validate_dayobs_range(start_raw: str, end_raw: str) -> str | None:
    """Return an error detail for a bad dayobs range, or None if it's fine."""
    try:
        start, end = int(start_raw), int(end_raw)
    except (ValueError, TypeError):
        return None

    for label, value in (("dayObsStart", start), ("dayObsEnd", end)):
        try:
            dayobs_int_to_date(value)
        except ValueError:
            logger.warning(f"Rejected malformed {label}: {value!r}")
            return f"Invalid {label}: {value!r}"

    if start > end:
        logger.warning(f"Rejected inverted dayobs range: dayObsStart={start}, dayObsEnd={end}")
        return f"dayObsStart ({start}) must not be after dayObsEnd ({end})"

    return None


class DayobsValidationMiddleware(BaseHTTPMiddleware):
    """Reject malformed or inverted dayObsStart/dayObsEnd ranges from a
    request querystring.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        start_raw = request.query_params.get("dayObsStart")
        end_raw = request.query_params.get("dayObsEnd")

        if start_raw and end_raw:
            # Only run if a match is found, else let it fall through to 404
            matched = any(route.matches(request.scope)[0] == Match.FULL for route in request.app.routes)
            if matched:
                error = _validate_dayobs_range(start_raw, end_raw)
                if error:
                    return JSONResponse(status_code=422, content={"detail": error})

        return await call_next(request)
