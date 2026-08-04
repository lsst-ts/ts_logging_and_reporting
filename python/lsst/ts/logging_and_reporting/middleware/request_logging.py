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
import time
import uuid

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from lsst.ts.logging_and_reporting.utils.logging_config import set_request_id

logger = logging.getLogger(__name__)

SLOW_REQUEST_SECONDS = 10.0
"""Request duration that is logged as a warning instead of debug."""

_UNLOGGED_PATHS = frozenset({"/health"})
"""Paths logged by neither line: the Kubernetes readiness and liveness
probes hit ``/health`` often enough to drown everything else."""


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log every request as it arrives and how long it took.

    The arrival line is INFO and carries the query string.
    The completion line is DEBUG, escalating to a warning past
    `SLOW_REQUEST_SECONDS`; that line repeats the query so a
    slow request is actionable on its own.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path.rstrip("/")
        if path in _UNLOGGED_PATHS:
            return await call_next(request)

        # Set before call_next: the downstream task copies the context
        # as it is created, so every record logged while serving this
        # request carries the ID.
        set_request_id(uuid.uuid4().hex[:8])
        query = request.url.query or "no parameters"
        logger.info(f"Fetching {path} with {query}")

        started = time.monotonic()
        try:
            response = await call_next(request)
        except Exception:
            logger.warning(
                f"Request {path} with {query} failed after "
                f"{time.monotonic() - started:.1f}s"
            )
            raise
        elapsed = time.monotonic() - started

        if elapsed >= SLOW_REQUEST_SECONDS:
            logger.warning(
                f"Slow request: {path} with {query} responded "
                f"{response.status_code} in {elapsed:.1f}s"
            )
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"{path} responded {response.status_code} in {elapsed:.1f}s")
        return response
