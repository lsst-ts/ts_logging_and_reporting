# This file is part of ts_logging_and_reporting.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""Cached adapter for ConsDB exposure records."""

import functools

from lsst.ts.logging_and_reporting.adapters.base_adapters import (
    InstrumentDayobsCachedAdapter,
)
from lsst.ts.logging_and_reporting.adapters.base_clients import SqlClient
from lsst.ts.logging_and_reporting.adapters.mixins import ConsdbSqlMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.auth import Server

# Transformed-EFD channels folded into each exposure record via a LEFT
# JOIN, per instrument. An empty/absent list means no transformed-EFD join.
TRANSFORMED_EFD_FIELDS = {
    "lsstcam": ["mt_salindex112_temperature_0_mean"],
    "latiss": [],
}

# Deployments carrying the transformed-EFD schema; the join is omitted
# everywhere else.
TRANSFORMED_EFD_DEPLOYMENTS = {Server.usdfdev, Server.usdf}


def transformed_efd_available() -> bool:
    """Whether this deployment exposes the transformed-EFD schema."""
    return Server.get_url() in TRANSFORMED_EFD_DEPLOYMENTS


class ConsdbExposuresAdapter(ConsdbSqlMixin, SqlClient, InstrumentDayobsCachedAdapter):
    """Caches the exposure record (exposure ⋈ quicklook ⋈ transformed-EFD).

    One cache entry serves two endpoints: `ExposuresService` projects
    the curated night-summary columns, `DataLogService` returns the full
    record.

    Transformed-EFD is only deployed at USDF, so this adapter behaves
    slightly differently based on EXTERNAL_INSTANCE_URL
    """

    name = "consdb_exposures"

    def _fetch_run(self, instrument: str, run_start: int, run_end: int) -> dict[int, list[dict]]:
        if transformed_efd_available() and (fields := TRANSFORMED_EFD_FIELDS.get(instrument)):
            transformed_efd_columns = "".join(f", f.{field}" for field in fields)
            transformed_efd_join = (
                f"LEFT JOIN efd_{instrument}.exposure_efd f ON e.exposure_id = f.exposure_id"
            )
        else:
            transformed_efd_columns = ""
            transformed_efd_join = ""
        sql = f"""
            SELECT e.*, q.*{transformed_efd_columns}
            FROM cdb_{instrument}.exposure e
            LEFT JOIN cdb_{instrument}.visit1_quicklook q
                ON e.exposure_id = q.visit_id
            {transformed_efd_join}
            WHERE {run_start} <= e.day_obs AND e.day_obs <= {run_end}
        """
        return self._partition_by_field(self._query(" ".join(sql.split())))


@functools.cache
def get_consdb_exposures_adapter() -> ConsdbExposuresAdapter:
    return ConsdbExposuresAdapter(get_redis_client())
