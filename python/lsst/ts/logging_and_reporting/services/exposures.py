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

import numpy as np
import pandas as pd
from astropy.time import Time

from lsst.ts.logging_and_reporting.adapters.almanac import get_almanac_adapter
from lsst.ts.logging_and_reporting.adapters.consdb_exposures import get_consdb_exposures_adapter
from lsst.ts.logging_and_reporting.adapters.rubin_nights_dome import get_rubin_nights_dome_adapter
from lsst.ts.logging_and_reporting.adapters.visit_overhead import get_visit_overhead_adapter
from lsst.ts.logging_and_reporting.services.base_service import Service
from lsst.ts.logging_and_reporting.utils.dayobs import add_or_subtract_dayobs_days, dayobs_at
from lsst.ts.logging_and_reporting.utils.obs_status import almanac_to_unix_ms

logger = logging.getLogger(__name__)

SECONDS_IN_AN_HOUR = 3600

# Empty-range time-accounting result.
_EMPTY_TIME_ACCOUNTING = {
    "sum_overhead_with_filter_change": 0.0,
    "sum_overhead_without_filter_change": 0.0,
    "sum_visit_gap_with_filter_change": 0.0,
    "sum_visit_gap_without_filter_change": 0.0,
}

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


def _twilight_windows_by_dayobs(almanac_info: list[dict]) -> dict[int, tuple[int, int]]:
    """Map each visit dayobs to its 12-degree twilight window (Unix ms).

    The almanac labels a night's record with the dayobs of its *morning*
    twilight boundary -- one day after the day_obs visits are tagged with
    -- so each record maps back to ``dayobs - 1`` to match ``day_obs``.
    """
    windows = {}
    for night in almanac_info:
        visit_dayobs = add_or_subtract_dayobs_days(night["dayobs"], -1)
        windows[visit_dayobs] = (
            almanac_to_unix_ms(night["twilight_evening_12deg"]),
            almanac_to_unix_ms(night["twilight_morning_12deg"]),
        )
    return windows


def _obs_start_tai_to_utc_ms(obs_start: pd.Series) -> pd.Series:
    """Convert TAI observation-start timestamps to Unix milliseconds (UTC).

    Missing input values are returned as ``NaN``.
    """
    result = pd.Series(np.nan, index=obs_start.index, dtype="float64")
    valid = obs_start.notna()
    if valid.any():
        tai_times = Time(obs_start[valid].astype(str).tolist(), format="isot", scale="tai")
        result.loc[valid] = tai_times.utc.unix * 1000
    return result


def _compute_filter_changed(visits_sorted: pd.DataFrame) -> pd.Series:
    """Flag visits whose band differs from the immediately preceding visit."""
    band_prev = visits_sorted["band"].shift(1)
    band_changed = (visits_sorted["band"] != band_prev) & ~(visits_sorted["band"].isna() & band_prev.isna())
    band_changed.iloc[0] = False
    return band_changed


def _sum_on_sky_within_twilight(visits: pd.DataFrame, almanac_info: list[dict]) -> dict[str, float]:
    """Summarize on-sky overhead and visit-gap hours within twilight.

    Returns four hour-valued sums, split by whether the band changed from
    the previous visit. Only visits that can see sky and start within
    their night's 12-degree twilight window are counted. ``overhead`` and
    ``visit_gap`` are stored in seconds and converted to hours once, at
    the end, to avoid compounding rounding error.
    """
    visits_sorted = visits.sort_values("obs_start")

    windows = _twilight_windows_by_dayobs(almanac_info)
    window_start_ms = (
        visits_sorted["day_obs"].map(lambda d: windows.get(d, (np.nan, np.nan))[0]).astype(float)
    )
    window_end_ms = visits_sorted["day_obs"].map(lambda d: windows.get(d, (np.nan, np.nan))[1]).astype(float)

    obs_start_utc_ms = _obs_start_tai_to_utc_ms(visits_sorted["obs_start"])

    in_twilight = (obs_start_utc_ms >= window_start_ms) & (obs_start_utc_ms <= window_end_ms)
    base_mask = visits_sorted["can_see_sky"].fillna(False).astype(bool) & in_twilight

    filter_changed = _compute_filter_changed(visits_sorted)
    with_change_mask = base_mask & filter_changed
    without_change_mask = base_mask & ~filter_changed

    overhead = visits_sorted["overhead"].fillna(0)
    visit_gap = visits_sorted["visit_gap"].fillna(0)

    sums_sec = {
        "sum_overhead_with_filter_change": float(overhead[with_change_mask].sum()),
        "sum_overhead_without_filter_change": float(overhead[without_change_mask].sum()),
        "sum_visit_gap_with_filter_change": float(visit_gap[with_change_mask].sum()),
        "sum_visit_gap_without_filter_change": float(visit_gap[without_change_mask].sum()),
    }
    return {key: round(float(total / SECONDS_IN_AN_HOUR), 2) for key, total in sums_sec.items()}


class ExposuresService(Service):
    """Serves /exposures: ConsDB exposures plus night-summary metrics.

    The exposures come from the cached ConsDB adapter (projected to the
    curated `EXPOSURE_COLUMNS`, with derived counts and durations). Dome
    open/close hours and twilight time accounting come from the
    ``rubin_nights`` helpers; their failures degrade to error fields in
    the response rather than failing the request.
    """

    def handle_request(self, day_obs_start: int, day_obs_end: int, instrument: str) -> dict:
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
        """
        inclusive_end = add_or_subtract_dayobs_days(day_obs_end, -1)
        fetched = self.fetch_concurrently(
            {
                "consdb": lambda: self.adapters["consdb"].fetch(instrument, day_obs_start, inclusive_end),
                "dome": lambda: self.adapters["dome"].fetch(day_obs_start, inclusive_end),
                "overhead": lambda: self.adapters["overhead"].fetch(instrument, day_obs_start, inclusive_end),
            }
        )
        # Exposures are the core payload, so a ConsDB failure fails the
        # request; dome and time accounting degrade to error fields instead.
        if isinstance(fetched["consdb"], Exception):
            raise fetched["consdb"]
        response = self.collate_response(fetched["consdb"])
        response.update(self._dome_hours(fetched["dome"]))
        response.update(self._time_accounting(fetched["overhead"], day_obs_start, day_obs_end))
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

    def _dome_hours(self, dome_result: dict | Exception) -> dict:
        """Dome open/close times and per-night open/closed hours.

        ``dome_result`` is the dome adapter's per-dayobs sessions, or the
        exception it raised (degraded to an error field).
        """
        open_dome_times_records, open_dome_hours_records, open_dome_error = [], {}, None
        try:
            if isinstance(dome_result, Exception):
                raise dome_result
            per_day = dome_result
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
        self, overhead_result: dict | Exception, day_obs_start: int, day_obs_end: int
    ) -> dict:
        """Twilight-windowed on-sky time accounting for the range.

        ``overhead_result`` is the overhead adapter's per-visit rows, or
        the exception it raised (degraded to an error field). The twilight
        windows come from the almanac adapter (fetched only when there are
        overhead rows); the reduction runs over the whole range so the
        filter-change split is correct across night boundaries.
        """
        night_time_on_sky_sums, time_accounting_error = None, None
        try:
            if isinstance(overhead_result, Exception):
                raise overhead_result
            per_day = overhead_result
            rows = [record for dayobs in sorted(per_day) for record in per_day[dayobs]]
            if not rows:
                night_time_on_sky_sums = dict(_EMPTY_TIME_ACCOUNTING)
            else:
                almanac = self.adapters["almanac"].fetch(day_obs_start, day_obs_end)
                almanac_info = [almanac[dayobs] for dayobs in sorted(almanac)]
                night_time_on_sky_sums = _sum_on_sky_within_twilight(pd.DataFrame(rows), almanac_info)
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
            "overhead": get_visit_overhead_adapter(),
            "almanac": get_almanac_adapter(),
        }
    )
