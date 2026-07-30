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

"""Service for the /jira-tickets endpoint.

The ``JiraObsCachedAdapter`` caches range-independent ticket records
for all instruments per dayobs; instrument filtering, deduplication,
and the range-dependent ``isNew`` flag are handled here at collation
time.
"""

import functools
import logging
from datetime import datetime

from lsst.ts.logging_and_reporting.adapters.jira_obs import JiraObsCachedAdapter, get_jira_obs_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import (
    add_or_subtract_dayobs_days,
    get_utc_datetime_from_dayobs_str,
)

logger = logging.getLogger(__name__)


INSTRUMENTS = {
    "LATISS": "AuxTel",
    "LSSTCam": "Simonyi",
}

INSTRUMENT_EXCLUDE_MAP = {
    "LSSTCam": ["LATISS"],
}


def filter_tickets_by_instrument(tickets, instrument, exclude=False):
    """Filters a list of JIRA tickets by instrument system match.

    For each ticket, checks if any of the system names in the ticket's
    'system' field contains (string includes) the instrument name
    or its corresponding value from the INSTRUMENTS dictionary.

    Parameters
    ----------
    tickets : `list[dict]`
        List of JIRA ticket dictionaries,
        each containing at least 'system' and 'key' fields.
    instrument : `str`
        The instrument name to filter tickets by.
    exclude : `bool`
        If True, return tickets that do **not** match the instrument;
        if False (default), return tickets that match.

    Returns
    -------
    filtered_tickets : `list[dict]`
        The filtered list of tickets.
    """

    def matches(ticket):
        obj_system_list = ticket["system"]
        search_terms = (instrument, INSTRUMENTS[instrument])
        return any(term in system for term in search_terms for system in obj_system_list)

    if exclude:
        return [ticket for ticket in tickets if not matches(ticket)]
    return [ticket for ticket in tickets if matches(ticket)]


class JiraTicketsService(Service):
    """Collates OBS Jira tickets for /jira-tickets."""

    def __init__(self, jira_obs_adapter: JiraObsCachedAdapter | None = None) -> None:
        self.jira_obs_adapter = jira_obs_adapter if jira_obs_adapter is not None else get_jira_obs_adapter()

    def handle(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
        """Return OBS tickets for the range and instrument.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the API
            contract — the frontend sends end + 1 day).
        instrument : `str`
            Instrument to filter by (e.g. ``LSSTCam``).
        """
        per_day = self.jira_obs_adapter.fetch(day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1))

        # A ticket can sit in several dayobs buckets (created one day,
        # updated another) — keep the first occurrence of each key.
        deduplicated = {}
        for dayobs in sorted(per_day):
            for ticket in per_day[dayobs]:
                deduplicated.setdefault(ticket["key"], ticket)
        tickets = list(deduplicated.values())

        window_start = get_utc_datetime_from_dayobs_str(day_obs_start)
        window_end = get_utc_datetime_from_dayobs_str(day_obs_end)
        for ticket in tickets:
            created_utc = datetime.fromisoformat(ticket.pop("created_utc"))
            ticket["isNew"] = window_start <= created_utc < window_end

        if instrument in INSTRUMENT_EXCLUDE_MAP:
            for excluded_instrument in INSTRUMENT_EXCLUDE_MAP[instrument]:
                tickets = filter_tickets_by_instrument(tickets, instrument=excluded_instrument, exclude=True)
        else:
            tickets = filter_tickets_by_instrument(tickets, instrument=instrument)

        response = self.collate_response(tickets)
        logger.debug(
            f"Collated {len(response['issues'])} Jira tickets "
            f"for dayObsStart: {day_obs_start}, dayObsEnd: {day_obs_end} "
            f"and instrument: {instrument}"
        )
        return response

    def collate_response(self, data: list[dict]) -> dict:
        return {"issues": data}


@functools.cache
def get_jira_tickets_service() -> JiraTicketsService:
    return JiraTicketsService()
