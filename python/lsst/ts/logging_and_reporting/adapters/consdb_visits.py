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

"""Cached adapter for ConsDB visit records."""

import functools

from lsst.ts.logging_and_reporting.adapters.base_adapters import InstrumentDayobsCachedAdapter
from lsst.ts.logging_and_reporting.adapters.base_clients import SqlClient
from lsst.ts.logging_and_reporting.adapters.mixins import ConsdbSqlMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client


class ConsdbVisitsAdapter(ConsdbSqlMixin, SqlClient, InstrumentDayobsCachedAdapter):
    """Caches the raw visit record (visit1 ⋈ visit1_quicklook) per night.

    Feeds the visit-map endpoints. Cached un-augmented: the rubin_nights
    augmentation is pure local compute and runs in `VisitMapsService` on
    read, so both map forms derive from one entry.
    """

    name = "consdb_visits"

    def _fetch_run(self, instrument: str, run_start: int, run_end: int) -> list[dict]:
        sql = f"""
            SELECT v.*, q.*
            FROM cdb_{instrument}.visit1 v
            LEFT JOIN cdb_{instrument}.visit1_quicklook q
                ON v.visit_id = q.visit_id
            WHERE {run_start} <= v.day_obs AND v.day_obs <= {run_end}
        """
        return self._query(" ".join(sql.split()))


@functools.cache
def get_consdb_visits_adapter() -> ConsdbVisitsAdapter:
    return ConsdbVisitsAdapter(get_redis_client())
