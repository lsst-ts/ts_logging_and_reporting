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
"""Cached adapter for per-visit slew/overhead times."""

import functools
import logging

import numpy as np
import pandas as pd
import rubin_nights.augment_visits as rn_aug
import rubin_nights.rubin_scheduler_addons as rn_sch

from lsst.ts.logging_and_reporting.adapters.base_adapters import (
    InstrumentDayobsCachedAdapter,
)
from lsst.ts.logging_and_reporting.adapters.consdb_exposures import (
    get_consdb_exposures_adapter,
)
from lsst.ts.logging_and_reporting.adapters.mixins import RubinNightsClientsMixin
from lsst.ts.logging_and_reporting.redis_client import get_redis_client
from lsst.ts.logging_and_reporting.utils.serialization import make_json_safe

logger = logging.getLogger(__name__)

# Slew-model tuning, in seconds; MAX_SCATTER caps the modelled slew
# against the measured visit gap.
WAIT_BEFORE_SLEW = 1.45
SETTLE = 2.0
MAX_SCATTER = 120.0

# Per-visit columns the time-accounting reduction reads.
OVERHEAD_COLUMNS = [
    "day_obs",
    "obs_start",
    "can_see_sky",
    "band",
    "overhead",
    "visit_gap",
]


class VisitOverheadAdapter(RubinNightsClientsMixin, InstrumentDayobsCachedAdapter):
    """Caches per-visit slew/overhead per instrument and dayobs.

    This adapter runs augment + slew-model per night and caches the per-visit
    overhead rows. Visits come from the ConsDB exposures adapter
    (composed), so no second ConsDB query is issued.
    """

    name = "visit_overhead"

    def __init__(self, redis_client, exposures_adapter):
        super().__init__(redis_client)
        self._exposures_adapter = exposures_adapter

    def _fetch_run(self, instrument: str, run_start: int, run_end: int) -> dict[int, list[dict]]:
        per_day = self._exposures_adapter.fetch(instrument, run_start, run_end)
        exposures = [record for dayobs in sorted(per_day) for record in per_day[dayobs]]
        if not exposures:
            return {}
        # Slew/overhead modelling is Simonyi (lsstcam) kinematics.
        visits = rn_aug.augment_visits(pd.DataFrame(exposures), "lsstcam", skip_rs_columns=True)
        visits, _ = rn_sch.add_model_slew_times(
            visits,
            self._efd_client,
            model_settle=WAIT_BEFORE_SLEW + SETTLE,
            dome_crawl=False,
        )
        slew = np.where(np.isnan(visits.slew_model.values), 0, visits.slew_model.values) + MAX_SCATTER
        visits["overhead"] = np.min([slew, visits.visit_gap.values], axis=0)
        return self._partition_by_field(make_json_safe(visits[OVERHEAD_COLUMNS].to_dict(orient="records")))


@functools.cache
def get_visit_overhead_adapter() -> VisitOverheadAdapter:
    return VisitOverheadAdapter(get_redis_client(), get_consdb_exposures_adapter())
