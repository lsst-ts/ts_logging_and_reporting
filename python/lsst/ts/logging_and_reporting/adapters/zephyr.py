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

"""Cached adapter for test-case names from the Zephyr Scale API.

See https://support.smartbear.com/zephyr-scale-cloud/api-docs/ for
the upstream API.
"""

import functools
import logging

import requests

from lsst.ts.logging_and_reporting.adapters.base_adapters import IdCachedAdapter
from lsst.ts.logging_and_reporting.adapters.base_clients import RestClient
from lsst.ts.logging_and_reporting.adapters.mixins import MutableDataMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client

logger = logging.getLogger(__name__)

ZEPHYR_SCALE_URL = "https://api.zephyrscale.smartbear.com/v2"


class ZephyrAdapter(MutableDataMixin, RestClient, IdCachedAdapter):
    """Fetches and caches Zephyr Scale test-case names by BLOCK key.

    Suffixed keys (``BLOCK-T123_a``) are variants of one test case
    that Zephyr Scale stores only under the parent key
    (``BLOCK-T123``), so the cache is keyed by parent and one entry
    serves every suffix. Keys with no test case are cached as
    ``None`` so an unknown key does not trigger an upstream query on
    every request.
    """

    name = "zephyr"
    auth_source = "zephyr"

    @property
    def server(self) -> str:
        return self._server_url or ZEPHYR_SCALE_URL

    @staticmethod
    def _parent_key(key: str) -> str:
        return key.split("_", 1)[0]

    def fetch_by_ids(self, ids: list[str]) -> dict[str, str | None]:
        parents = list(dict.fromkeys(self._parent_key(key) for key in ids))
        by_parent = self._fetch_cached(parents)
        return {key: by_parent[self._parent_key(key)] for key in ids}

    def _fetch_from_source(self, ids: list[str]) -> dict[str, str | None]:
        results: dict[str, str | None] = {}
        for key in ids:
            logger.debug(f"Fetching Zephyr test case {key}")
            try:
                results[key] = self._get_json(f"{self.server}/testcases/{key}")["name"]
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    logger.warning(f"Zephyr test case {key} not found")
                    results[key] = None
                else:
                    raise
        return results


@functools.cache
def get_zephyr_adapter() -> ZephyrAdapter:
    return ZephyrAdapter(get_redis_client())
