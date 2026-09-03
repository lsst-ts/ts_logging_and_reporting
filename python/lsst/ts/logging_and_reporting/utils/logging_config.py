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

NO_TRACE_ID = "-"
"""Stand-in for records logged outside a traced unit of work."""

_trace_id: ContextVar[str] = ContextVar("trace_id", default=NO_TRACE_ID)

LOG_FORMAT = "%(levelname)s [%(name)s] [%(trace_id)s] %(message)s"

DEFAULT_LOG_LEVEL = "INFO"

LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
"""Levels ``LOG_LEVEL`` accepts. Matches uvicorn's allowable log levels."""


def log_level() -> str:
    """The level named by ``LOG_LEVEL``, or `DEFAULT_LOG_LEVEL`."""
    level = os.environ.get("LOG_LEVEL", "").strip().upper()
    return level if level in LOG_LEVELS else DEFAULT_LOG_LEVEL


def set_trace_id(trace_id: str) -> None:
    """Tag every record logged from here on with ``trace_id``."""
    _trace_id.set(trace_id)


def current_trace_id() -> str:
    """The ``trace_id`` in scope, or `NO_TRACE_ID` outside a traced unit."""
    return _trace_id.get()


class TraceIdFilter(logging.Filter):
    """Give every record a ``trace_id``, so `LOG_FORMAT` can use it.

    Attached to the handler rather than to a logger: a handler filter
    sees every record that reaches it, whichever logger emitted it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = current_trace_id()
        return True


def configure_logging() -> None:
    """Configure logging for the current process."""
    requested = os.environ.get("LOG_LEVEL", "").strip()
    logging.basicConfig(level=log_level(), format=LOG_FORMAT)
    for handler in logging.getLogger().handlers:
        if not any(isinstance(f, TraceIdFilter) for f in handler.filters):
            handler.addFilter(TraceIdFilter())
    # Warned about only once logging is configured, and at a level the
    # default lets through, so the fallback is not silent.
    if requested and requested.upper() not in LOG_LEVELS:
        logging.getLogger(__name__).warning(
            f"Unrecognised LOG_LEVEL {requested!r}, using {DEFAULT_LOG_LEVEL}"
        )
