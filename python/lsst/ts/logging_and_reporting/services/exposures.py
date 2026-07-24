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

"""Service for the /exposures endpoint."""

import functools
import logging
from typing import Any

import pandas as pd

from lsst.ts.logging_and_reporting.adapters.consdb_exposures import get_consdb_exposures_adapter
from lsst.ts.logging_and_reporting.adapters.rubin_nights_dome import get_rubin_nights_dome_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.services.rubin_nights_service import get_time_accounting
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days, dayobs_at

logger = logging.getLogger(__name__)

# Curated column set surfaced by /exposures, projected from the full
# exposure record the adapter caches.
EXPOSURE_COLUMNS = [
    "exposure_id",
    "exposure_name",
    "exp_time",
    "img_type",
    "observation_reason",
    "science_program",
    "target_name",
    "can_see_sky",
    "band",
    "obs_start",
    "physical_filter",
    "day_obs",
    "seq_num",
    "obs_end",
    "exp_midpt_mjd",
    "obs_start_mjd",
    "obs_end_mjd",
    "s_dec",
    "s_ra",
    "sky_rotation",
    "zero_point_median",
    "visit_id",
    "pixel_scale_median",
    "psf_sigma_median",
]


def _compute_closed_hours(totals: dict[str, Any], current_dayobs: int, now_utc: pd.Timestamp) -> float:
    """Closed dome hours for one aggregated per-night row.

    For completed nights this is ``night_hours - open_hours``. For the
    current night the meaning is "closed so far", so it is measured
    against the twilight time elapsed since sunset:
    ``max(0, elapsed_twilight_hours - open_hours)``.
    """
    sunset12_utc = pd.to_datetime(totals["sunset12"], utc=True)
    sunrise12_utc = pd.to_datetime(totals["sunrise12"], utc=True)

    night_in_progress = (
        totals["day_obs"] == current_dayobs
        and pd.notna(sunset12_utc)
        and pd.notna(sunrise12_utc)
        and sunset12_utc <= now_utc <= sunrise12_utc
    )

    if not night_in_progress:
        return totals["night_hours"] - totals["open_hours"]

    elapsed_hours = (now_utc - sunset12_utc).total_seconds() / 3600
    return max(0.0, elapsed_hours - totals["open_hours"])


def _aggregate_dome_hours(per_day: dict[int, list[dict]]) -> dict[int, dict]:
    """Aggregate per-dayobs dome sessions into per-night open/closed hours.

    Each night's ``night_hours`` is the max across its sessions,
    ``open_hours`` the sum; ``sunset12``/``sunrise12`` come from the
    first session. Nights with no sessions are omitted.
    """
    # Take one instant and derive its dayobs from it, so the closed-hours
    # "night in progress" check stays consistent across the noon rollover.
    now_utc = pd.Timestamp.now(tz="UTC")
    current_dayobs = dayobs_at(now_utc)
    hours = {}
    for dayobs, records in per_day.items():
        if not records:
            continue
        totals = {
            "day_obs": dayobs,
            "night_hours": max(
                (r["night_hours"] for r in records if r.get("night_hours") is not None), default=0.0
            ),
            "open_hours": sum((r.get("open_hours") or 0.0) for r in records),
            "sunset12": records[0].get("sunset12"),
            "sunrise12": records[0].get("sunrise12"),
        }
        hours[dayobs] = {
            "night_hours": totals["night_hours"],
            "open_hours": totals["open_hours"],
            "sunset12": totals["sunset12"],
            "sunrise12": totals["sunrise12"],
            "closed_hours": _compute_closed_hours(totals, current_dayobs, now_utc),
        }
    return hours


class ExposuresService(Service):
    """Serves /exposures: ConsDB exposures plus night-summary metrics.

    The exposures come from the cached ConsDB adapter (projected to the
    curated `EXPOSURE_COLUMNS`, with derived counts and durations). Dome
    open/close hours and twilight time accounting come from the
    ``rubin_nights`` helpers; their failures degrade to error fields in
    the response rather than failing the request.
    """

    def handle_request(self, day_obs_start: int, day_obs_end: int, instrument: str, auth_token: str) -> dict:
        """Return exposures and night-summary metrics for the range.

        Parameters
        ----------
        day_obs_start : `int`
            Inclusive lower bound of the dayobs range.
        day_obs_end : `int`
            Exclusive upper bound of the dayobs range (the API
            contract — the frontend sends end + 1 day).
        instrument : `str`
            Instrument to query (e.g. ``LSSTCam``).
        auth_token : `str`
            Token for the dome and time-accounting queries.
        """
        per_day = self.adapters["consdb"].fetch(
            instrument, day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1)
        )
        response = self.collate_response(per_day)
        response.update(self._dome_hours(day_obs_start, day_obs_end))
        response.update(
            self._time_accounting(day_obs_start, day_obs_end, instrument, response["exposures"], auth_token)
        )
        logger.debug(
            f"Fetched {response['exposures_count']} exposures for dayObsStart: {day_obs_start}, "
            f"dayObsEnd: {day_obs_end} and instrument: {instrument}"
        )
        return response

    def collate_response(self, data: dict[int, list[dict]]) -> dict:
        # seq_num is a per-night sequence, so order by (day_obs, seq_num):
        # iterate dayobs in order, sorting each night's records by seq_num.
        records = []
        for dayobs in sorted(data):
            records.extend(sorted(data[dayobs], key=lambda record: record.get("seq_num") or 0))
        exposures = [{column: record.get(column) for column in EXPOSURE_COLUMNS} for record in records]
        on_sky = [exposure for exposure in exposures if exposure.get("can_see_sky")]
        return {
            "exposures": exposures,
            "exposures_count": len(exposures),
            "sum_exposure_time": sum(exposure.get("exp_time") or 0 for exposure in exposures),
            "on_sky_exposures_count": len(on_sky),
            "total_on_sky_exposure_time": sum(exposure.get("exp_time") or 0 for exposure in on_sky),
        }

    def _dome_hours(self, day_obs_start: int, day_obs_end: int) -> dict:
        """Dome open/close times and per-night open/closed hours."""
        open_dome_times_records, open_dome_hours_records, open_dome_error = [], {}, None
        try:
            per_day = self.adapters["dome"].fetch(day_obs_start, add_or_subtract_dayobs_days(day_obs_end, -1))
            open_dome_times_records = [record for dayobs in sorted(per_day) for record in per_day[dayobs]]

            try:
                open_dome_hours_records = _aggregate_dome_hours(per_day)
            except Exception as e:
                logger.error(f"Error aggregating open/close dome times: {e}", exc_info=True)
                open_dome_hours_records = None
                open_dome_error = "Failed to aggregate dome open hours"

        except Exception as e:
            logger.error(
                f"Error getting open/close dome times from rubin_nights through EFD: {e}", exc_info=True
            )
            open_dome_times_records = None
            open_dome_hours_records = None
            open_dome_error = "Failed to retrieve dome open/close times"

        return {
            "open_dome_times": open_dome_times_records,
            "day_obs_open_dome_hours": open_dome_hours_records,
            "open_dome_error": open_dome_error,
        }

    def _time_accounting(
        self, day_obs_start: int, day_obs_end: int, instrument: str, exposures: list[dict], auth_token: str
    ) -> dict:
        """Twilight-windowed on-sky time accounting for the night."""
        night_time_on_sky_sums, time_accounting_error = None, None
        try:
            night_time_on_sky_sums = get_time_accounting(
                day_obs_start, day_obs_end, instrument, exposures, auth_token
            )
        except Exception as e:
            logger.error(f"Error computing time accounting in /exposures: {e}", exc_info=True)
            time_accounting_error = "Failed to compute night time accounting"

        return {
            "night_on_sky_time_accounting": night_time_on_sky_sums,
            "time_accounting_error": time_accounting_error,
        }


@functools.cache
def get_exposures_service() -> ExposuresService:
    return ExposuresService(
        adapters={
            "consdb": get_consdb_exposures_adapter(),
            "dome": get_rubin_nights_dome_adapter(),
        }
    )
