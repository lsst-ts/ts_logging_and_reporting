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

"""Mixins shared across adapter families.

Each concrete adapter is composed from a cache base (`*CachedAdapter`),
a transport client (`*Client`), and zero or more of these mixins. The
mixins carry cross-adapter behaviour — TTL policy, upstream auth, query
shaping — without duplicating it in every adapter.
"""

import datetime as dt
import functools
import logging
from typing import Any

from fastapi import HTTPException
from rubin_nights.connections import get_clients

from lsst.ts.logging_and_reporting.cache_ttl import (
    MUTABLE_TTL_REDIS,
    TODAY_TTL_REDIS,
)
from lsst.ts.logging_and_reporting.utils.auth import (
    AUTH_SOURCES,
    get_jira_hostname,
    retrieve_access_token,
)
from lsst.ts.logging_and_reporting.utils.dayobs import current_dayobs

logger = logging.getLogger(__name__)


class MutableDataMixin:
    """TTL policy for adapters whose records can still change.

    Log messages, tickets, and test cases can be added or edited at
    any time, so entries get the shorter `MUTABLE_TTL_REDIS` instead
    of `HISTORIC_TTL_REDIS`, mirroring the mutable endpoint list in
    ``CacheControlMiddleware``. Works under either cache base class:
    for dayobs-keyed adapters today's entry keeps `TODAY_TTL_REDIS`;
    ID-keyed entries have no today.
    """

    def _ttl(self, key) -> int:
        if isinstance(key, int) and key == current_dayobs():
            return TODAY_TTL_REDIS
        return MUTABLE_TTL_REDIS


class JiraApiMixin:
    """Server resolution and Basic-auth headers for the Jira API."""

    auth_source = "jira"

    @property
    def server(self) -> str:
        return self._server_url or f"https://{get_jira_hostname()}"

    def _request_headers(self) -> dict:
        return {
            "Authorization": f"Basic {self._get_token()}",
            "content-type": "application/json",
        }

    @staticmethod
    def get_system_names(jira_system_field: Any) -> list[str]:
        """Extract system names from the OBS systems custom field.

        Jira returns the field as nested lists/dicts; every ``name``
        value found anywhere in the structure is a system or subsystem
        name (e.g. ``"Simonyi"``).
        """
        systems = []

        def walk(obj):
            if isinstance(obj, dict):
                if "name" in obj:
                    systems.append(obj["name"])
                for value in obj.values():
                    walk(value)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(jira_system_field)
        return systems


class ConsdbSqlMixin:
    """Request validation and row shaping for ConsDB SQL adapters.

    Mixed in ahead of a `SqlClient` transport and an
    `InstrumentDayobsCachedAdapter` cache base. The instrument and
    dayobs are interpolated into raw SQL, so `fetch` rejects unknown
    instruments and malformed dayobs before they reach `_fetch_run`.
    """

    def fetch(self, instrument: str, start_dayobs: int, end_dayobs: int) -> dict[int, list[dict]]:
        self._validate_instrument(instrument)
        self._validate_dayobs(start_dayobs)
        self._validate_dayobs(end_dayobs)
        return super().fetch(instrument, start_dayobs, end_dayobs)

    def _validate_instrument(self, instrument: str) -> str:
        """Return the normalised instrument, or raise 422 if unrecognised."""
        normalised = instrument.lower()
        if normalised not in self.INSTRUMENTS:
            raise HTTPException(status_code=422, detail=f"Unknown instrument: {instrument!r}")
        return normalised

    @staticmethod
    def _validate_dayobs(dayobs: int) -> None:
        try:
            dt.datetime.strptime(str(dayobs), "%Y%m%d")
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid dayobs: {dayobs!r}")

    def _rows_from_result(self, result: dict) -> list[dict]:
        # The quicklook join yields duplicate column names; keep the first
        # non-null so a join null never clobbers a valid primary value.
        records = []
        duplicate_columns = set()
        for row in result["data"]:
            record: dict = {}
            for column, value in zip(result["columns"], row):
                if column in record:
                    duplicate_columns.add(column)
                    if record[column] is None and value is not None:
                        record[column] = value
                else:
                    record[column] = value
            records.append(record)
        if duplicate_columns:
            logger.debug(f"Merged duplicate ConsDB columns: {', '.join(sorted(duplicate_columns))}")
        return records


class RubinNightsClientsMixin:
    """Lazy, process-cached access to the rubin_nights connection clients.

    Built once so credential discovery does not repeat on every fetch.
    Adapters that only need the EFD use `_efd_client`; the context feed
    needs the full dict and uses `_clients`.
    """

    @functools.cached_property
    def _clients(self) -> dict:
        token = retrieve_access_token(AUTH_SOURCES["rsp"])
        return get_clients(auth_token=token)

    @functools.cached_property
    def _efd_client(self):
        return self._clients["efd"]
