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

"""Cached adapter for locally computed almanac (sun/moon) data."""

import datetime as dt
import functools
import logging
import warnings
from typing import Any

import astropy.coordinates
from astroplan import Observer
from astropy.time import Time

from lsst.ts.logging_and_reporting.base_adapters import DayobsCachedAdapter
from lsst.ts.logging_and_reporting.cache_ttl import HISTORIC_TTL_REDIS
from lsst.ts.logging_and_reporting.redis_client import get_redis_client

logger = logging.getLogger(__name__)


def _iso(time: Time) -> str:
    return Time(time, precision=0).iso


class AlmanacCachedAdapter(DayobsCachedAdapter):
    """Computes and caches per-night sun and moon events.

    There is no upstream service: the events are computed locally with
    astroplan. Records are keyed by the dayobs of the morning twilight
    boundary — the record for a dayobs describes the night *ending* on
    that date's morning, so the night of observing dayobs N is cached
    under N + 1.

    Times are UTC ISO strings truncated to whole seconds.
    """

    name = "almanac"

    def __init__(self, redis: Any, site: str = "Rubin"):
        super().__init__(redis)
        self._site = site

    @functools.cached_property
    def _observer(self) -> Observer:
        with warnings.catch_warnings(action="ignore"):
            location = astropy.coordinates.EarthLocation.of_site(self._site)
            return Observer(location, timezone="Chile/Continental")

    def _ttl(self, dayobs: int) -> int:
        """Always the historic TTL — ephemeris data never changes,
        even for today's entry."""
        return HISTORIC_TTL_REDIS

    def _fetch_from_source(self, dayobs_list: list[int]) -> dict[int, dict]:
        return {dayobs: self._compute_night(dayobs) for dayobs in dayobs_list}

    def _compute_night(self, dayobs: int) -> dict:
        logger.debug(f"Computing almanac events for dayobs {dayobs}")
        observer = self._observer
        with warnings.catch_warnings(action="ignore"):
            date = dt.datetime.strptime(str(dayobs), "%Y%m%d")
            midnight = observer.midnight(
                Time(date, format="datetime", scale="utc", location=observer.location),
                which="next",
            )
            nau_twilight_morning = observer.twilight_morning_nautical(midnight, which="next")
            nau_twilight_evening = observer.twilight_evening_nautical(midnight, which="previous")
            return {
                "dayobs": dayobs,
                "night_hours": (nau_twilight_morning - nau_twilight_evening).to_value("hr"),
                "twilight_evening_18deg": _iso(
                    observer.twilight_evening_astronomical(midnight, which="previous")
                ),
                "twilight_morning_18deg": _iso(
                    observer.twilight_morning_astronomical(midnight, which="next")
                ),
                "twilight_evening_12deg": _iso(nau_twilight_evening),
                "twilight_morning_12deg": _iso(nau_twilight_morning),
                "twilight_evening_6deg": _iso(observer.twilight_evening_civil(midnight, which="previous")),
                "twilight_morning_6deg": _iso(observer.twilight_morning_civil(midnight, which="next")),
                "twilight_evening_0deg": _iso(observer.sun_set_time(midnight, which="previous")),
                "twilight_morning_0deg": _iso(observer.sun_rise_time(midnight, which="next")),
                "moon_rise_time": _iso(observer.moon_rise_time(midnight, which="nearest")),
                "moon_set_time": _iso(observer.moon_set_time(midnight, which="nearest")),
                "moon_illumination": f"{observer.moon_illumination(midnight):.0%}",
            }


@functools.cache
def get_almanac_adapter() -> AlmanacCachedAdapter:
    return AlmanacCachedAdapter(get_redis_client())
