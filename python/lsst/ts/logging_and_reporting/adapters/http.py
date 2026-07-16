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

"""Base class for cached adapters backed by REST APIs."""

import logging
from abc import ABC
from typing import Any

import requests

from lsst.ts.logging_and_reporting.utils import (
    AUTH_SOURCES,
    Server,
    get_auth_header,
    retrieve_access_token,
)
from lsst.ts.logging_and_reporting.web_app.base_adapter import CachedAdapter

logger = logging.getLogger(__name__)


class RestCachedAdapter(CachedAdapter, ABC):
    """`CachedAdapter` for upstream REST APIs.

    Provides server URL resolution, service-account authentication,
    and JSON GET requests. Auth tokens are resolved per request from
    the source configured by ``auth_source`` (environment variable or
    RSP utilities), so token rotation needs no restart.

    HTTP or connection failures raise, matching the cache-loop
    contract that any upstream error fails the whole fetch.
    """

    auth_source = "rsp"
    """Key into ``AUTH_SOURCES`` naming the token used upstream."""

    CONNECT_TIMEOUT = 5.05
    """Connection timeout in seconds."""

    READ_TIMEOUT = 20
    """Read timeout in seconds."""

    def __init__(self, redis: Any, server_url: str | None = None):
        super().__init__(redis)
        self.server = server_url or Server.get_url()

    def _get_token(self) -> str:
        return retrieve_access_token(AUTH_SOURCES[self.auth_source])

    def _get_json(self, url: str, params: dict | None = None) -> Any:
        """GET ``url`` and return the decoded JSON body.

        Parameters
        ----------
        url : `str`
            Full URL of the API endpoint.
        params : `dict`, optional
            Query parameters; entries with ``None`` values are
            dropped.

        Raises
        ------
        requests.HTTPError
            If the response status is an error.
        requests.ConnectionError
            If the server is unreachable.
        """
        response = requests.get(
            url,
            params={key: value for key, value in (params or {}).items() if value is not None},
            headers=get_auth_header(self._get_token()),
            timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
        )
        response.raise_for_status()
        return response.json()
