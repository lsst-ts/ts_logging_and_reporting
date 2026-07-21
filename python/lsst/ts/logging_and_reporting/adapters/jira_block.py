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

"""Cached adapter for BLOCK ticket summaries from Jira.

See https://developer.atlassian.com/cloud/jira/platform/rest/v3 for
the upstream API.
"""

import functools
import logging

from lsst.ts.logging_and_reporting.adapters.base_clients import RestClient
from lsst.ts.logging_and_reporting.adapters.mixins import JiraApiMixin, MutableDataMixin
from lsst.ts.logging_and_reporting.web_app.base_adapters import IdCachedAdapter
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class JiraBlockAdapter(JiraApiMixin, MutableDataMixin, RestClient, IdCachedAdapter):
    """Fetches and caches BLOCK ticket summaries by issue key.

    Keys the search does not return are cached as ``None`` so an
    unknown key does not trigger an upstream query on every request.
    """

    name = "jira_block"

    def _fetch_from_source(self, ids: list[str]) -> dict[str, str | None]:
        logger.debug(f"Fetching BLOCK ticket summaries for {ids}")
        jql_query = f"project = BLOCK AND key in ({','.join(ids)})"
        response = self._get_json(
            f"{self.server}/rest/api/latest/search/jql",
            params={"jql": jql_query, "fields": "summary"},
        )
        summaries = {issue["key"]: issue["fields"]["summary"] for issue in response.get("issues", [])}
        return {key: summaries.get(key) for key in ids}


@functools.cache
def get_jira_block_adapter() -> JiraBlockAdapter:
    return JiraBlockAdapter(get_redis_client())
