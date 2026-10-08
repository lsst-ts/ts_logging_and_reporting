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
"""Startup check that the environment the backend needs is configured.

Run by each process entrypoint before it starts serving, so a
misconfigured deployment fails at startup rather than on the first
request that needs the missing value.
"""

import logging
import os

from .auth import Server

logger = logging.getLogger(__name__)

DEBUG_ENV_VAR = "ND_DEBUG"
"""Environment variable that switches on development mode."""

REQUIRED_ENV_VARS = (
    "ACCESS_TOKEN",
    "JIRA_API_TOKEN",
    "ZEPHYR_API_TOKEN",
    "JIRA_API_HOSTNAME",
    "EXTERNAL_INSTANCE_URL",
)
"""Variables that must be set in every mode."""

PRODUCTION_ENV_VARS = (
    "RUBIN_SIM_DATA_DIR",
    "AWS_SHARED_CREDENTIALS_FILE",
    "S3_ENDPOINT_URL",
    "LSST_DISABLE_BUCKET_VALIDATION",
    "REDIS_HOST",
)
"""Variables that must be set in production, but only warn in
development mode, where they may not be found."""


def debug_mode() -> bool:
    """Whether `DEBUG_ENV_VAR` switches on development mode.

    Unset, empty and ``"0"`` leave it off; any other value turns it
    on.
    """
    return os.environ.get(DEBUG_ENV_VAR, "").strip() not in ("", "0")


def _unset(names: tuple[str, ...]) -> list[str]:
    """The entries of ``names`` that are unset or blank."""
    return [name for name in names if not os.environ.get(name, "").strip()]


def check_environment() -> None:
    """Fail if the environment is missing anything the backend needs.

    Every problem found is reported together in one log message.

    Raises
    ------
    SystemExit
        If any variable in `REQUIRED_ENV_VARS` is unset, if
        ``EXTERNAL_INSTANCE_URL`` is not a known `Server`, or, outside
        development mode, if any variable in `PRODUCTION_ENV_VARS` is
        unset.
    """
    problems = [f"{name} is unset" for name in _unset(REQUIRED_ENV_VARS)]

    instance_url = os.environ.get("EXTERNAL_INSTANCE_URL", "").strip()
    if instance_url and instance_url not in Server.get_all():
        problems.append(f"EXTERNAL_INSTANCE_URL {instance_url!r} is not a known deployment")

    production_unset = _unset(PRODUCTION_ENV_VARS)

    if production_unset and debug_mode():
        logger.warning(
            f"{DEBUG_ENV_VAR} is set; continuing without {', '.join(production_unset)}. "
            f"Features that depend on them will fail."
        )
    else:
        problems.extend(f"{name} is unset" for name in production_unset)

    if problems:
        logger.critical(f"Environment misconfigured: {'; '.join(problems)}")
        raise SystemExit(1)
