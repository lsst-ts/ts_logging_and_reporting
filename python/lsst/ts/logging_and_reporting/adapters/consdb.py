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

"""Cached adapter for the Consolidated Database (ConsDB)."""

import datetime as dt
import functools
import logging
from abc import ABC

from fastapi import HTTPException

from lsst.ts.logging_and_reporting.adapters.http import SqlClient
from lsst.ts.logging_and_reporting.web_app.base_adapter import InstrumentDayobsCachedAdapter
from lsst.ts.logging_and_reporting.web_app.redis_client import get_redis_client

logger = logging.getLogger(__name__)

# Transformed-EFD channels folded into each exposure record via a LEFT
# JOIN, per instrument. An empty/absent list means no EFD join.
EFD_FIELDS = {
    "lsstcam": ["mt_salindex112_temperature_0_mean"],
    "latiss": [],
}


class ConsdbSqlAdapter(SqlClient, InstrumentDayobsCachedAdapter, ABC):
    """Shared base for the ConsDB instrument adapters.

    Adds request validation on top of the instrument+dayobs cache: the
    instrument and dayobs are interpolated into raw SQL, so only known
    instruments and well-formed dayobs are allowed to reach `_fetch_run`.
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


class ConsdbExposuresAdapter(ConsdbSqlAdapter):
    """Caches the full exposure record (exposure ⋈ quicklook ⋈ EFD).

    One cache entry serves both endpoints: `ExposuresService` projects
    the curated night-summary columns, `DataLogService` returns the full
    record. The transformed-EFD channels for the instrument are folded in
    via a LEFT JOIN so no second query or merge is needed.
    """

    name = "consdb_exposures"

    def _fetch_run(self, instrument: str, run_start: int, run_end: int) -> list[dict]:
        efd_fields = EFD_FIELDS.get(instrument, [])
        efd_columns = "".join(f", f.{field}" for field in efd_fields)
        efd_join = (
            f"LEFT JOIN efd_{instrument}.exposure_efd f ON e.exposure_id = f.exposure_id"
            if efd_fields
            else ""
        )
        sql = f"""
            SELECT e.*, q.*{efd_columns}
            FROM cdb_{instrument}.exposure e
            LEFT JOIN cdb_{instrument}.visit1_quicklook q
                ON e.exposure_id = q.visit_id
            {efd_join}
            WHERE {run_start} <= e.day_obs AND e.day_obs <= {run_end}
        """
        return self._query(" ".join(sql.split()))


class ConsdbVisitsAdapter(ConsdbSqlAdapter):
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
def get_consdb_exposures_adapter() -> ConsdbExposuresAdapter:
    return ConsdbExposuresAdapter(get_redis_client())


@functools.cache
def get_consdb_visits_adapter() -> ConsdbVisitsAdapter:
    return ConsdbVisitsAdapter(get_redis_client())
