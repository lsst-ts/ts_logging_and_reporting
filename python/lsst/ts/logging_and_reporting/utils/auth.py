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
"""Authentication tokens, headers, and server/host resolution."""

import os

from fastapi import HTTPException, Request

AUTH_SOURCES = {
    "rsp": {
        "env_var": "ACCESS_TOKEN",
        "label": "RSP",
        "use_rsp_utils": True,
    },
    "jira": {
        "env_var": "JIRA_API_TOKEN",
        "label": "Jira",
        "use_rsp_utils": False,
    },
    "zephyr": {
        "env_var": "ZEPHYR_API_TOKEN",
        "label": "Zephyr",
        "use_rsp_utils": False,
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


def retrieve_access_token(config: dict, request: Request = None) -> str:
    """Retrieve an authentication token using a configurable sequence of
    fallback methods.

    This function is framework-agnostic and can be used anywhere token
    retrieval is needed, without relying on FastAPI.

    Retrieval order
    ---------------
    1. Preferred RSP notebook API
    (``lsst.rsp._services.RSPDiscovery.get_token``) if enabled.
    2. Fallback notebook API (``lsst.rsp.utils.get_access_token``) for
    backward compatibility.
    3. Environment variable specified in the config.
    4. Authorization header from the provided request, if any.

    Parameters
    ----------
    config : `dict`
        Configuration for the authentication source. Must contain keys:
        - ``"use_rsp_utils"`` (`bool`)
        - ``"env_var"`` (`str`)
        - ``"label"`` (`str`)
    request : `fastapi.Request`, optional
        FastAPI request object used to extract the token from headers.
        Default is None.

    Returns
    -------
    `str`
        The resolved authentication token.

    Raises
    ------
    HTTPException
        If no token could be retrieved by any method.
    """

    # Try RSP notebook utils (only if enabled)
    if config.get("use_rsp_utils"):
        # Preferred API
        try:
            from lsst.rsp._services import RSPDiscovery

            token = RSPDiscovery.get_token()
            if token:
                return token
        except (ImportError, Exception):
            pass

        # Backward compatibility fallback
        try:
            import lsst.rsp.utils

            token = lsst.rsp.utils.get_access_token()
            if token:
                return token
        except ImportError:
            pass

    # Try env variable
    env_token = os.getenv(config["env_var"])
    if env_token is not None:
        return env_token

    # Try request headers
    if request is not None:
        auth_header = request.headers.get("Authorization")
        if auth_header and " " in auth_header:
            return auth_header.split(" ")[1]

    raise HTTPException(
        status_code=401,
        detail=f"{config['label']} authentication token could not be retrieved by any method.",
    )


def get_access_token(source: str = "rsp"):
    """FastAPI dependency factory that provides an authentication token for a
    given source.

    This is a thin wrapper around ``retrieve_access_token`` that returns a
    callable suitable for ``fastapi.Depends``. Each call to the dependency
    will attempt to resolve a token according to the configured source.

    Parameters
    ----------
    source : `str`, optional
        The authentication source identifier. Must be a key in
        ``AUTH_SOURCES``. Defaults to ``"rsp"``.

    Returns
    -------
    callable
        A dependency function that FastAPI will call per request. The returned
        function accepts an optional ``fastapi.Request`` and returns a token.

    Raises
    ------
    HTTPException
        If a token cannot be retrieved by any method. The exception message
        includes the configured source label for clarity.

    Notes
    -----
    This function is a factory and must be called when used with
    ``fastapi.Depends``, e.g. ``Depends(get_access_token("jira"))``.

    Usage in FastAPI routes:

        from fastapi import Depends

        @app.get("/example")
        def endpoint(auth_token: str = Depends(get_access_token("jira"))):
            return {"token": auth_token}
    """
    config = AUTH_SOURCES[source]

    def dependency(request: Request = None):
        """
        FastAPI dependency function for retrieving an authentication token.

        Parameters
        ----------
        request : `fastapi.Request`, optional
            The incoming request object, used to extract the token from
            headers.

        Returns
        -------
        `str`
            Authentication token retrieved using ``retrieve_access_token``.

        Raises
        ------
        HTTPException
            If no token could be resolved.
        """
        return retrieve_access_token(config, request)

    return dependency


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
