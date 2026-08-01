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

"""Authentication tokens, headers, and server/host resolution."""

import logging
import os

from fastapi import HTTPException

logger = logging.getLogger(__name__)

AUTH_SOURCES = {
    "rsp": {
        "env_var": "ACCESS_TOKEN",
        "label": "RSP",
    },
    "jira": {
        "env_var": "JIRA_API_TOKEN",
        "label": "Jira",
    },
    "zephyr": {
        "env_var": "ZEPHYR_API_TOKEN",
        "label": "Zephyr",
    },
}


# Servers we might use
class Server:
    """Deployment URLs.

    Currently active host detected via EXTERNAL_INSTANCE_URL.
    """

    summit = "https://summit-lsp.lsst.codes"
    usdfdev = "https://usdf-rsp-dev.slac.stanford.edu"
    usdf = "https://usdf-rsp.slac.stanford.edu"
    tucson = "https://tucson-teststand.lsst.codes"
    base = "https://base-lsp.lsst.codes"

    @classmethod
    def get_all(cls):
        return [
            value for value in cls.__dict__.values() if isinstance(value, str) and value.startswith("https")
        ]

    @classmethod
    def get_url(cls):
        env_var_name = "EXTERNAL_INSTANCE_URL"
        current = os.environ.get(env_var_name)

        match current:
            case Server.summit:
                return Server.summit
            case Server.usdfdev:
                return Server.usdfdev
            case Server.usdf:
                return Server.usdf
            case Server.tucson:
                return Server.tucson
            case Server.base:
                return Server.base
            case _:
                raise ValueError(f"Unset or invalid {env_var_name}: {current}")


def retrieve_access_token(config: dict) -> str:
    """Return the service-account token named by ``config["env_var"]``.

    Raises
    ------
    HTTPException
        500 if the variable is unset — callers do not authenticate, so
        this is a misconfigured deployment, not a bad request.
    """
    env_token = os.getenv(config["env_var"])
    if env_token is not None:
        return env_token

    logger.error(f"{config['label']} service-account token is unset ({config['env_var']} not configured)")
    raise HTTPException(status_code=500, detail="Server configuration error")


def get_auth_header(token: str | None):
    """Construct an HTTP Authorization header using a bearer token.

    Parameters
    ----------
    token : `str` or `None`
        The authentication token to include in the header.

    Returns
    -------
    `dict`
        A dictionary containing the ``Authorization`` header with the
        bearer token.

    Raises
    ------
    ValueError
        If ``token`` is None or empty.

    Notes
    -----
    This function does not retrieve tokens. Token acquisition should be
    handled upstream
    """
    if not token:
        raise ValueError("Auth token is required")
    return {"Authorization": f"Bearer {token}"}


def get_jira_hostname():
    """Retrieve the Jira API hostname from environment configuration.

    This function is intended for use as a FastAPI dependency to supply the
    Jira service hostname to endpoints or downstream clients.

    The hostname is read from the ``JIRA_API_HOSTNAME`` environment variable.

    Returns
    -------
    `str`
        The Jira API hostname.

    Raises
    ------
    HTTPException
        If the hostname is not defined in the environment. Returns a 500
        status code indicating a server configuration error.

    Notes
    -----
    This function only retrieves configuration and does not perform any
    network validation of the hostname.
    """
    hostname = os.getenv("JIRA_API_HOSTNAME")
    if not hostname:
        raise HTTPException(
            status_code=500,
            detail="Jira hostname not configured",
        )
    return hostname
