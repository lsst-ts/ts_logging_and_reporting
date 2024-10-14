import datetime as dt
import warnings

import astropy.coordinates
from astroplan import Observer
from astropy.time import Time
from lsst.ts.logging_and_reporting.source_adapters import SourceAdapter


# Compare to https://www.timeanddate.com/astronomy/chile/santiago
class Almanac(SourceAdapter):
    """Get almanac data for a night given a dayobs.
    A dayobs is the date of the start of an observing night. Therefore
    for sunrise and morning twilight we get time on the date AFTER dayobs.
    For sunset and evening twilight we get time on date of dayobs.
    For moonrise/set we get the time nearest to the midnight after day_ob.
    Times in UTC.
    """

    def __init__(
        self,
        *,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        site="Rubin",
    ):
        super().__init__(max_dayobs=max_dayobs, min_dayobs=min_dayobs)

        dayobs = self.min_dayobs
        # Allow formats: int, YYYY-MM-DD, YYYYMMDD
        dobs = str(dayobs).replace("-", "")
        astro_day = dt.datetime.strptime(dobs, "%Y%m%d").date()
        astro_date = dt.datetime.strptime(dobs, "%Y%m%d")

        with warnings.catch_warnings(action="ignore"):
            self.loc = astropy.coordinates.EarthLocation.of_site(site)
            self.observer = Observer(self.loc, timezone="Chile/Continental")
            self.astro_day = astro_day
            self.astro_midnight = self.observer.midnight(
                Time(
                    astro_date,
                    format="datetime",
                    scale="utc",
                    location=self.loc,
                ),
                which="next",
            )
            self.get_moon()
            self.get_sun()

    def get_moon(self):
        self.moon_rise_time = self.observer.moon_rise_time(
            self.astro_midnight, which="nearest"
        )
        self.moon_set_time = self.observer.moon_set_time(
            self.astro_midnight, which="nearest"
        )

        # Percent of moon lit
        self.moon_illum = self.observer.moon_illumination(self.astro_midnight)

    def get_sun(self):
        # ast(ronoimical) twilight: -18 degrees)
        obs = self.observer
        self.ast_twilight_morning = obs.twilight_morning_astronomical(
            self.astro_midnight, which="next"
        )
        self.ast_twilight_evening = obs.twilight_evening_astronomical(
            self.astro_midnight, which="previous"
        )

        # nau(tical) twilight: -12 degrees)
        self.nau_twilight_morning = self.observer.twilight_morning_nautical(
            self.astro_midnight, which="next"
        )
        self.nau_twilight_evening = self.observer.twilight_evening_nautical(
            self.astro_midnight, which="previous"
        )

        # civ(il) twilight: -6 degrees)
        self.civ_twilight_morning = self.observer.twilight_morning_civil(
            self.astro_midnight, which="next"
        )
        self.civ_twilight_evening = self.observer.twilight_evening_civil(
            self.astro_midnight, which="previous"
        )

        self.sun_rise_time = self.observer.sun_rise_time(
            self.astro_midnight, which="next"
        )
        self.sun_set_time = self.observer.sun_set_time(
            self.astro_midnight, which="previous"
        )

    @property
    def night_hours(self):
        day_delta = self.ast_twilight_morning - self.ast_twilight_evening
        return day_delta.to_value("hr")

    @property
    def as_dict(self):
        data_dict = {
            "": "(times in UTC)",
            "Moon Rise": self.moon_rise_time.iso,
            "Moon Set": self.moon_set_time.iso,
            "Moon Illumination": f"{self.moon_illum:.0%}",
            "Astronomical Twilight (morning)": self.ast_twilight_morning.iso,
            "Astronomical Twilight (evening)": self.ast_twilight_evening.iso,
            "Solar Midnight": self.astro_midnight.iso,
            "Nautical Twilight (morning)": self.nau_twilight_morning.iso,
            "Nautical Twilight (evening)": self.nau_twilight_evening.iso,
            "Civil Twilight (morning)": self.civ_twilight_morning.iso,
            "Civil Twilight (evening)": self.civ_twilight_evening.iso,
            "Sun Rise": self.sun_rise_time.iso,
            "Sun Set": self.sun_set_time.iso,
        }
        help_dict = {
            "": "",
            "Moon Set": "",
            "Moon Rise": "",
            "Moon Illumination": "(% illuminated)",
            "Astronomical Twilight (evening)": "(-18 degrees)",
            "Astronomical Twilight (morning)": "(-18 degrees)",
            "Solar Midnight": "",
            "Nautical Twilight (evening)": "(-12 degrees)",
            "Nautical Twilight (morning)": "(-12 degrees)",
            "Civil Twilight (evening)": "(-6 degrees)",
            "Civil Twilight (morning)": "(-6 degrees)",
            "Sun Set": "",
            "Sun Rise": "",
        }
        return data_dict, help_dict
