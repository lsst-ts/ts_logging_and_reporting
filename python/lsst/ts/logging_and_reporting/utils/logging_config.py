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

"""Logging configuration shared by every process this app starts."""

import logging
import os
from contextvars import ContextVar

NO_REQUEST_ID = "-"
"""Stand-in for records logged outside a request."""

_request_id: ContextVar[str] = ContextVar("request_id", default=NO_REQUEST_ID)

LOG_FORMAT = "%(levelname)s [%(name)s] [%(request_id)s] %(message)s"


def set_request_id(request_id: str) -> None:
    """Tag every record logged from here on with ``request_id``."""
    _request_id.set(request_id)


def current_request_id() -> str:
    """The request ID in scope, or `NO_REQUEST_ID` outside a request."""
    return _request_id.get()


class RequestIdFilter(logging.Filter):
    """Give every record a ``request_id``, so `LOG_FORMAT` can use it.

    Attached to the handler rather than to a logger: a handler filter
    sees every record that reaches it, whichever logger emitted it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = current_request_id()
        return True


def configure_logging() -> None:
    """Configure logging for the current process."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format=LOG_FORMAT,
    )
    for handler in logging.getLogger().handlers:
        if not any(isinstance(f, RequestIdFilter) for f in handler.filters):
            handler.addFilter(RequestIdFilter())
