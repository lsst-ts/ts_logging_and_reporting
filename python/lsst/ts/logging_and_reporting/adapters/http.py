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

"""Base classes for cached adapters backed by REST APIs."""

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
from lsst.ts.logging_and_reporting.web_app.base_adapter import DayobsCachedAdapter

logger = logging.getLogger(__name__)


class RestClient:
    """Mixin adding authenticated REST access to a cached adapter.

    Provides server URL resolution, service-account authentication,
    and JSON GET requests. Auth tokens are resolved per request from
    the source configured by ``auth_source`` (environment variable or
    RSP utilities), so token rotation needs no restart.

    HTTP or connection failures raise, matching the cache-loop
    contract that any upstream error fails the whole fetch.

    Combine with a cache base class: `RestCachedAdapter` pairs it
    with `DayobsCachedAdapter` for dayobs-keyed adapters; ID-keyed
    adapters pair it with `IdCachedAdapter` themselves.
    """

    auth_source = "rsp"
    """Key into ``AUTH_SOURCES`` naming the token used upstream."""

    CONNECT_TIMEOUT = 5.05
    """Connection timeout in seconds."""

    READ_TIMEOUT = 20
    """Read timeout in seconds."""

    MAX_RECORDS = 9000
    """Upper bound on records fetched by one paged request."""

    def __init__(self, redis: Any, server_url: str | None = None):
        super().__init__(redis)
        self._server_url = server_url

    @property
    def server(self) -> str:
        return self._server_url or Server.get_url()

    def _get_token(self) -> str:
        return retrieve_access_token(AUTH_SOURCES[self.auth_source])

    def _request_headers(self) -> dict:
        return get_auth_header(self._get_token())

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
            headers=self._request_headers(),
            timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
        )
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
        response = requests.post(
            url,
            json=json_body,
            headers=self._request_headers(),
            timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
        )
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


class RestCachedAdapter(RestClient, DayobsCachedAdapter, ABC):
    """`DayobsCachedAdapter` for upstream REST APIs."""


class SqlClient(RestClient):
    """REST client for ConsDB's SQL query endpoint.

    Give it SQL, get back row dicts: adds ``/consdb/query`` execution
    and result shaping on top of `RestClient`. Combine with a cache
    base (e.g. `InstrumentDayobsCachedAdapter`) to build an adapter.
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
        """Zip the ``columns``/``data`` payload into row dicts.

        Assumes distinct column names; callers whose queries join tables
        with overlapping names override this.
        """
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
