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

"""Cached adapter for OBS tickets from the Jira REST API.

See https://developer.atlassian.com/cloud/jira/platform/rest/v3 for
the upstream API.
"""

import datetime as dt
import functools
import logging
from typing import Any

from pytz import timezone

from lsst.ts.logging_and_reporting.adapters.http import RestCachedAdapter
from lsst.ts.logging_and_reporting.utils import (
    add_or_subtract_dayobs_days,
    current_dayobs_utc,
    get_jira_hostname,
    get_utc_datetime_from_dayobs_str,
)
from lsst.ts.logging_and_reporting.web_app.base_adapter import contiguous_runs
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)

OBS_SYSTEMS_FIELD = "customfield_10476"
TIME_LOST_FIELD = "customfield_10106"

JQL_DATE_FORMAT = "%Y-%m-%d %H:%M"
TIMESTAMP_INPUT_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
TIMESTAMP_OUTPUT_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_system_names(jira_system_field: Any) -> list[str]:
    """Extract system names from the OBS systems custom field.

    Jira returns the field as nested lists/dicts; every ``name`` value
    found anywhere in the structure is a system or subsystem name
    (e.g. ``"Simonyi"``).
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


class JiraObsCachedAdapter(RestCachedAdapter):
    """Fetches and caches OBS Jira tickets per dayobs.

    A ticket belongs to a dayobs bucket when it was created or last
    updated within that dayobs's noon-to-noon UTC window, matching the
    upstream JQL query. Jira records only the latest update time, so a
    ticket can appear in up to two buckets (its creation day and its
    last-update day); the service deduplicates by key at collation.

    Tickets for **all instruments** are cached together per dayobs —
    the service filters by instrument when collating. The
    range-dependent ``isNew`` flag is likewise derived by the service,
    from the cached ``created_utc`` field, so cached records stay
    range-independent.
    """

    name = "jira_obs"
    auth_source = "jira"

    EXCLUDED_STATUSES = ["Cancelled"]
    ISSUE_FIELDS = [
        "key",
        "summary",
        "updated",
        "created",
        "status",
        "system",
        OBS_SYSTEMS_FIELD,
        TIME_LOST_FIELD,
    ]

    @property
    def server(self) -> str:
        return self._server_url or f"https://{get_jira_hostname()}"

    def _request_headers(self) -> dict:
        return {
            "Authorization": f"Basic {self._get_token()}",
            "content-type": "application/json",
        }

    def _ttl(self, dayobs: int) -> int:
        """Always the short TTL.

        Tickets are mutable: a later edit moves a ticket's ``updated``
        timestamp (changing which buckets it belongs to) and can change
        its status, summary, or time lost — so historical buckets must
        not be cached for long.
        """
        return self.SHORT_TTL

    @functools.cached_property
    def _user_timezone(self):
        """Timezone of the Jira account, which JQL dates are read in."""
        myself = self._get_json(f"{self.server}/rest/api/latest/myself")
        return timezone(myself["timeZone"])

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, list[dict]]:
        results: dict[int, list[dict]] = {dayobs: [] for dayobs in dayobs_list}
        for run_start, run_end in contiguous_runs(dayobs_list):
            logger.debug(f"Fetching OBS Jira tickets for dayobs {run_start}..{run_end}")
            for issue in self._search_window(run_start, add_or_subtract_dayobs_days(run_end, 1)):
                record = self._to_record(issue)
                created = dt.datetime.strptime(issue["fields"]["created"], TIMESTAMP_INPUT_FORMAT)
                updated = dt.datetime.strptime(issue["fields"]["updated"], TIMESTAMP_INPUT_FORMAT)
                buckets = {
                    current_dayobs_utc(created.astimezone(dt.timezone.utc)),
                    current_dayobs_utc(updated.astimezone(dt.timezone.utc)),
                }
                for dayobs in buckets:
                    if dayobs in results:
                        results[dayobs].append(record)
        return results

    def _search_window(self, start_dayobs: int, end_dayobs: int) -> list[dict]:
        """Query OBS issues created or updated in ``[start, end)``.

        Both bounds are dayobs whose noon-UTC boundaries delimit the
        window; ``end_dayobs`` is exclusive.
        """
        start = self._jql_date(start_dayobs)
        end = self._jql_date(end_dayobs)
        status_exclusions = " ".join(f'AND status != "{status}"' for status in self.EXCLUDED_STATUSES)
        jql_query = (
            f"project = OBS {status_exclusions} "
            f'AND ((created >= "{start}" '
            f'AND created < "{end}") '
            f'OR (updated >= "{start}" '
            f'AND updated < "{end}"))'
        )
        response = self._get_json(
            f"{self.server}/rest/api/latest/search/jql",
            params={"jql": jql_query, "fields": ",".join(self.ISSUE_FIELDS)},
        )
        return response.get("issues", [])

    def _jql_date(self, dayobs: int) -> str:
        return (
            get_utc_datetime_from_dayobs_str(dayobs).astimezone(self._user_timezone).strftime(JQL_DATE_FORMAT)
        )

    def _to_record(self, issue: dict) -> dict:
        fields = issue["fields"]
        created = dt.datetime.strptime(fields["created"], TIMESTAMP_INPUT_FORMAT)
        updated = dt.datetime.strptime(fields["updated"], TIMESTAMP_INPUT_FORMAT)
        return {
            "key": issue["key"],
            "summary": fields["summary"],
            "updated": updated.strftime(TIMESTAMP_OUTPUT_FORMAT),
            "created": created.strftime(TIMESTAMP_OUTPUT_FORMAT),
            "status": fields["status"]["name"],
            "system": get_system_names(fields[OBS_SYSTEMS_FIELD]),
            "url": f"{self.server}/browse/{issue['key']}",
            "time_lost": fields[TIME_LOST_FIELD],
            "created_utc": created.astimezone(dt.timezone.utc).isoformat(),
        }


@functools.cache
def get_jira_obs_adapter() -> JiraObsCachedAdapter:
    return JiraObsCachedAdapter(get_redis_client())
