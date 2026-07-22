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

"""Service for the /block-details endpoint.

The endpoint is ID-driven rather than dayobs-driven: it receives a
list of BLOCK keys and resolves each against the source its pattern
selects — ``BLOCK-<n>`` against Jira, ``BLOCK-T<n>`` (optionally
``_x``-suffixed) against Zephyr Scale.
"""

import functools
import logging
import re

from fastapi import HTTPException

from lsst.ts.logging_and_reporting.adapters.jira_block import get_jira_block_adapter
from lsst.ts.logging_and_reporting.adapters.zephyr import get_zephyr_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils import get_jira_hostname

logger = logging.getLogger(__name__)

ZEPHYR_BLOCK_RE = re.compile(r"^BLOCK-T\d+(?:_[A-Za-z0-9]+)?$")
JIRA_BLOCK_RE = re.compile(r"^BLOCK-\d+$")

ZEPHYR_TEST_CASE_PATH = (
    "/projects/BLOCK?selectedItem=com.atlassian.plugins.atlassian-connect-plugin:"
    "com.kanoah.test-manager__main-project-page#!/v2/testCase/"
)


class BlockDetailsService(Service):
    """Resolves BLOCK keys to their summaries for /block-details.

    A failure of one source is reported in the response's ``errors``
    field while the other source's results are still returned; the
    request fails only when both sources fail.
    """

    def handle_request(self, keys: list[str]) -> dict:
        """Return summary, source, and URL per resolvable BLOCK key.

        Parameters
        ----------
        keys : `list` [`str`]
            BLOCK keys (e.g. ``BLOCK-704`` or ``BLOCK-T123_a``).
            Duplicates and keys matching neither pattern are dropped.
        """
        keys = list(dict.fromkeys(keys))
        source_keys = {
            "zephyr": [key for key in keys if ZEPHYR_BLOCK_RE.match(key)],
            "jira": [key for key in keys if JIRA_BLOCK_RE.match(key)],
        }

        summaries: dict[str, dict] = {"zephyr": {}, "jira": {}}
        errors: dict[str, str] = {}
        for source, wanted in source_keys.items():
            if not wanted:
                continue
            try:
                summaries[source] = self.adapters[source].fetch_by_ids(wanted)
            except HTTPException:
                raise
            except Exception as e:
                logger.exception(f"{source} fetch failed in /block-details")
                errors[source] = str(e)

        if "zephyr" in errors and "jira" in errors:
            raise HTTPException(status_code=500, detail="Both Zephyr and Jira requests failed.")

        response = self.collate_response({"summaries": summaries, "errors": errors})
        logger.debug(f"Collated {len(response['data'])} BLOCK details for keys: {keys}")
        return response

    def collate_response(self, data: dict) -> dict:
        """Decorate each resolved key with its source and URL.

        Keys their source did not resolve are omitted. Zephyr URLs
        point at the parent test case (the key without its ``_x``
        suffix).
        """
        blocks = {}
        for key, summary in data["summaries"]["zephyr"].items():
            if summary is None:
                continue
            parent = key.split("_", 1)[0]
            blocks[key] = {
                "key": key,
                "summary": summary,
                "source": "zephyr",
                "url": f"https://{get_jira_hostname()}{ZEPHYR_TEST_CASE_PATH}{parent}",
            }
        for key, summary in data["summaries"]["jira"].items():
            if summary is None:
                continue
            blocks[key] = {
                "key": key,
                "summary": summary,
                "source": "jira",
                "url": f"https://{get_jira_hostname()}/browse/{key}",
            }
        return {"data": blocks, "errors": data["errors"]}


@functools.cache
def get_block_details_service() -> BlockDetailsService:
    return BlockDetailsService(
        adapters={
            "zephyr": get_zephyr_adapter(),
            "jira": get_jira_block_adapter(),
        }
    )
