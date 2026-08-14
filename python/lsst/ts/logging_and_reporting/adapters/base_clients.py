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

"""Transport clients for adapters backed by REST APIs.

A client provides the request machinery (auth, timeouts, JSON GET/POST,
paging); a concrete adapter combines one with a cache base class and any
mixins. `RestClient` is the general JSON client; `SqlClient` adds ConsDB's
SQL query endpoint on top.
"""

import functools
import logging
import time
from typing import Any

import requests
import urllib3

from lsst.ts.logging_and_reporting.utils.auth import (
    AUTH_SOURCES,
    Server,
    get_auth_header,
    retrieve_access_token,
)

logger = logging.getLogger(__name__)


class RestClient:
    """Mixin adding authenticated REST access to a cached adapter.

    Provides server URL resolution, service-account authentication,
    and JSON GET requests. Auth tokens are read from the environment
    variable named by ``auth_source``.

    requests Exceptions thrown by this class are caught by
    ``Service.handle_request`` and turned into a generic 502
    """

    auth_source = "rsp"
    """Key into ``AUTH_SOURCES`` naming the token used upstream."""

    CONNECT_TIMEOUT = 5.05
    """Connection timeout in seconds."""

    READ_TIMEOUT = 20
    """Read timeout in seconds."""

    MAX_RECORDS = 9000
    """Upper bound on records fetched by one paged request."""

    SLOW_REQUEST_SECONDS = 10.0
    """Upstream call duration that is logged as a warning."""

    POOL_MAXSIZE = 20
    """Connections kept open per upstream host."""

    def __init__(self, redis: Any, server_url: str | None = None):
        super().__init__(redis)
        self._server_url = server_url

    @functools.cached_property
    def _session(self) -> requests.Session:
        """This client's pooled session.

        Built lazily because `WorkerPoolMixin.pool_preload` imports
        adapter modules into the forkserver, and a socket opened before
        the fork would be shared by every worker. ``read=0`` keeps a
        request that reached the server from being retried.
        """
        retry = urllib3.Retry(total=2, connect=2, read=0, status=0, backoff_factor=0.1)
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=self.POOL_MAXSIZE, max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @property
    def server(self) -> str:
        return self._server_url or Server.get_url()

    def _get_token(self) -> str:
        return retrieve_access_token(AUTH_SOURCES[self.auth_source])

    def _request_headers(self) -> dict:
        return get_auth_header(self._get_token())

    def _log_upstream(self, method: str, url: str, status: int | str, elapsed: float) -> None:
        """Record one upstream call, warning past `SLOW_REQUEST_SECONDS`."""
        if elapsed >= self.SLOW_REQUEST_SECONDS:
            logger.warning(f"Slow upstream {method} {url}: {status} in {elapsed:.1f}s")
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Upstream {method} {url}: {status} in {elapsed:.1f}s")

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
        started = time.monotonic()
        try:
            response = self._session.get(
                url,
                params={key: value for key, value in (params or {}).items() if value is not None},
                headers=self._request_headers(),
                timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
            )
        except requests.RequestException as err:
            # A connection error or timeout carries no response; name the
            # failure instead.
            status = getattr(err.response, "status_code", type(err).__name__)
            self._log_upstream("GET", url, status, time.monotonic() - started)
            raise
        self._log_upstream("GET", url, response.status_code, time.monotonic() - started)
        response.raise_for_status()
        return response.json()

    def _post_json(self, url: str, json_body: Any) -> Any:
        """POST ``json_body`` to ``url`` and return the decoded JSON body.

        Parameters
        ----------
        url : `str`
            Full URL of the API endpoint.
        json_body : `Any`
            Request payload, serialised as the JSON request body.

        Raises
        ------
        requests.HTTPError
            If the response status is an error.
        requests.ConnectionError
            If the server is unreachable.
        """
        started = time.monotonic()
        try:
            response = self._session.post(
                url,
                json=json_body,
                headers=self._request_headers(),
                timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
            )
        except requests.RequestException as err:
            status = getattr(err.response, "status_code", type(err).__name__)
            self._log_upstream("POST", url, status, time.monotonic() - started)
            raise
        self._log_upstream("POST", url, response.status_code, time.monotonic() - started)
        response.raise_for_status()
        return response.json()

    def _get_json_paged(self, url: str, params: dict, page_limit: int) -> list:
        """GET all pages of a record-list endpoint.

        Requests ``url`` repeatedly with ``limit``/``offset`` paging
        until a page comes back shorter than ``page_limit``, and
        returns the concatenated records. Fetching stops with a
        warning if ``MAX_RECORDS`` is reached.

        Parameters
        ----------
        url : `str`
            Full URL of the API endpoint.
        params : `dict`
            Query parameters; ``limit`` and ``offset`` are managed
            here and must not be present.
        page_limit : `int`
            Number of records requested per page.
        """
        params = dict(params, limit=page_limit, offset=0)
        records: list = []
        while True:
            page = self._get_json(url, params=dict(params))
            records.extend(page)
            if len(page) < page_limit:
                return records
            if len(records) >= self.MAX_RECORDS:
                logger.warning(
                    f"Fetch from {url} with params {params} hit the "
                    f"{self.MAX_RECORDS}-record cap; results truncated"
                )
                return records
            params["offset"] += len(page)


class SqlClient(RestClient):
    """RestClient for ConsDB's SQL query endpoint.

    Give it SQL, get back row dicts: adds ``/consdb/query`` execution
    and result shaping on top of `RestClient`.
    """

    def _query(self, sql: str) -> list[dict]:
        """POST ``sql`` to the ConsDB query endpoint and shape the rows.

        ConsDB reports SQL errors as an HTTP 500 with the Postgres text
        in the response body's ``message`` field; that message is logged
        here before the error propagates and the base `Service.handle`
        turns the requests failure into a 502.
        """
        url = f"{self.server}/consdb/query"
        try:
            result = self._post_json(url, {"query": sql})
        except requests.HTTPError as err:
            logger.error(f"ConsDB query failed: {self._error_message(err)}. SQL: {sql!r}")
            raise
        return self._rows_from_result(result)

    def _rows_from_result(self, result: dict) -> list[dict]:
        """Zip the ``columns``/``data`` payload into row dicts."""
        return [dict(zip(result["columns"], row)) for row in result["data"]]

    @staticmethod
    def _error_message(err: requests.HTTPError) -> str:
        """Best-effort extraction of ConsDB's Postgres error message."""
        response = err.response
        if response is None:
            return str(err)
        try:
            return response.json().get("message", response.text)
        except ValueError:
            return response.text
