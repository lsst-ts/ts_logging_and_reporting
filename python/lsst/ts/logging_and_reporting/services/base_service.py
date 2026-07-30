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
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class Service(ABC):
    """A thin collator over one or more adapters.

    Subclasses define their own typed ``__init__`` (named, typed adapter
    parameters) and ``handle`` signature; the typical implementation
    calls its adapter's ``fetch`` directly (single-adapter services) or
    fans several fetches out through the `fetch_concurrently` helper,
    then returns `collate_response` of the merged result.

    Instances are singletons created at application startup by each
    module's ``get_*_service`` factory and provided to endpoints via
    ``fastapi.Depends``.
    """

    @abstractmethod
    def handle(self, *args: Any) -> dict:
        """Do the work of one endpoint request.

        Each subclass defines its own typed signature (the base class
        deliberately does not prescribe parameters): fetch from the
        adapter(s), merge, and return ``collate_response(merged)``.
        """
        raise NotImplementedError

    def handle_request(self, *args: Any, **kwargs: Any) -> dict:
        """Call `handle`, converting errors to HTTP responses.

        Upstream request failures map to 502 Bad Gateway; anything
        else unexpected maps to 500.
        """
        try:
            return self.handle(*args, **kwargs)
        except HTTPException:
            raise
        except requests.RequestException as e:
            logger.exception(f"{type(self).__name__} upstream request failed")
            raise HTTPException(status_code=502, detail=f"Upstream query failed: {e}") from e
        except Exception as e:
            logger.exception(f"{type(self).__name__} request failed")
            raise HTTPException(status_code=500, detail=str(e)) from e

    def fetch_concurrently(self, tasks: dict[str, Callable[[], Any]]) -> dict[str, Any]:
        """Run each fetch thunk on a pool, returning result-or-exception.

        ``tasks`` maps a caller-chosen name to a zero-argument fetch
        callable. Each name maps back to its return value, or to the
        exception it raised so one failing fetch never aborts the others.
        """
        if not tasks:
            return {}
        results: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(task): name for name, task in tasks.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    results[name] = e
        return results

    @abstractmethod
    def collate_response(self, data: dict[int, Any]) -> dict:
        """Combine per-dayobs results into the final response payload.

        Implemented per service: most return per-dayobs data in a
        response-shaped dict; visualisation services build multi-night
        figures from the per-night data.
        """
        raise NotImplementedError
