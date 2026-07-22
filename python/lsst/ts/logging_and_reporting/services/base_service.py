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

"""Service base class for the adapter-backed endpoint architecture.

Services are thin collators:
each subclass calls its adapter(s), merges the per-dayobs results, and
returns via ``collate_response``. All caching lives in the adapter
layer; no Redis interaction occurs here.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

import requests
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.adapters.base_adapters import CachedAdapter

logger = logging.getLogger(__name__)


class Service(ABC):
    """A thin collator over one or more adapters.

    Subclasses define their own typed ``handle_request`` signature;
    the typical implementation calls each adapter's ``fetch`` (or the
    `fetch_all` helper), then returns `collate_response` of the merged
    result.

    Parameters
    ----------
    adapters : `dict` [`str`, `CachedAdapter`]
        The adapters this service collates, keyed by adapter name.
        Injected at construction; instances are singletons created at
        application startup and provided to endpoints via
        ``fastapi.Depends``.
    """

    def __init__(self, adapters: dict[str, CachedAdapter]):
        self.adapters = adapters

    @abstractmethod
    def handle_request(self, *args: Any) -> dict:
        """Handle one endpoint request.

        Each subclass defines its own typed signature (the base class
        deliberately does not prescribe parameters): fetch from the
        adapter(s), merge, and return ``collate_response(merged)``.
        """
        raise NotImplementedError

    def handle(self, *args: Any, **kwargs: Any) -> dict:
        """Call `handle_request`, converting errors to HTTP responses.

        Upstream request failures map to 502 Bad Gateway; anything
        else unexpected maps to 500.
        """
        try:
            return self.handle_request(*args, **kwargs)
        except HTTPException:
            raise
        except requests.RequestException as e:
            logger.exception(f"{type(self).__name__} upstream request failed")
            raise HTTPException(status_code=502, detail=f"Upstream query failed: {e}")
        except Exception as e:
            logger.exception(f"{type(self).__name__} request failed")
            raise HTTPException(status_code=500, detail=str(e))

    def fetch_all(self, start_dayobs: int, end_dayobs: int) -> dict[int, dict[str, Any]]:
        """Fetch the range from every adapter and merge per dayobs.

        Returns
        -------
        `dict` [`int`, `dict` [`str`, `Any`]]
            For each dayobs, the per-adapter results keyed by adapter
            name, e.g. ``{20250101: {"consdb": [...], "dome": [...]}}``.
        """
        merged: dict[int, dict[str, Any]] = {}
        for name, adapter in self.adapters.items():
            for dayobs, data in adapter.fetch(start_dayobs, end_dayobs).items():
                merged.setdefault(dayobs, {})[name] = data
        return merged

    @abstractmethod
    def collate_response(self, data: dict[int, Any]) -> dict:
        """Combine per-dayobs results into the final response payload.

        Implemented per service: most return per-dayobs data in a
        response-shaped dict; visualisation services build multi-night
        figures from the per-night data.
        """
        raise NotImplementedError
