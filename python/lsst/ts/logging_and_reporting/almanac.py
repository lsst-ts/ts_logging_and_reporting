import datetime as dt
import warnings

import astropy.coordinates
import pytz
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

        dayobs = self.max_dayobs  # min_dayobs
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
        def local(astropytime):
            tz = pytz.timezone("Chile/Continental")
            return (
                astropytime.to_datetime(timezone=tz)
                .replace(microsecond=0, tzinfo=None)
                .isoformat(sep=" ")
            )

        moon_rise_time = Time(self.moon_rise_time, precision=0).iso
        moon_set_time = Time(self.moon_set_time, precision=0).iso
        ast_twilight_morning = Time(self.ast_twilight_morning, precision=0).iso
        ast_twilight_evening = Time(self.ast_twilight_evening, precision=0).iso
        astro_midnight = Time(self.astro_midnight, precision=0).iso
        nau_twilight_morning = Time(self.nau_twilight_morning, precision=0).iso
        nau_twilight_evening = Time(self.nau_twilight_evening, precision=0).iso
        civ_twilight_morning = Time(self.civ_twilight_morning, precision=0).iso
        civ_twilight_evening = Time(self.civ_twilight_evening, precision=0).iso
        sun_rise_time = Time(self.sun_rise_time, precision=0).iso
        sun_set_time = Time(self.sun_set_time, precision=0).iso

        data_dict = {
            "": "(UTC time)",
            "Moon Rise": moon_rise_time,
            "Moon Set": moon_set_time,
            "Moon Illumination": f"{self.moon_illum:.0%}",
            "Morning Astronomical Twilight": ast_twilight_morning,
            "Evening Astronomical Twilight": ast_twilight_evening,
            "Solar Midnight": astro_midnight,
            "Morning Nautical Twilight": nau_twilight_morning,
            "Evening Nautical Twilight": nau_twilight_evening,
            "Morning Civil Twilight": civ_twilight_morning,
            "Evening Civil Twilight": civ_twilight_evening,
            "Sun Rise": sun_rise_time,
            "Sun Set": sun_set_time,
        }
        local_dict = {
            "": "(local time)",
            "Moon Rise": local(moon_rise_time),
            "Moon Set": local(moon_set_time),
            "Moon Illumination": local(f"{self.moon_illum:.0%}"),
            "Morning Astronomical Twilight": local(ast_twilight_morning),
            "Evening Astronomical Twilight": local(ast_twilight_evening),
            "Solar Midnight": local(astro_midnight),
            "Morning Nautical Twilight": local(nau_twilight_morning),
            "Evening Nautical Twilight": local(nau_twilight_evening),
            "Morning Civil Twilight": local(civ_twilight_morning),
            "Evening Civil Twilight": local(civ_twilight_evening),
            "Sun Rise": local(sun_rise_time),
            "Sun Set": local(sun_set_time),
        }
        help_dict = {
            "": "",
            "Moon Set": "",
            "Moon Rise": "",
            "Moon Illumination": "(% illuminated)",
            "Morning Astronomical Twilight": "(-18 degrees)",
            "Evening Astronomical Twilight": "(-18 degrees)",
            "Solar Midnight": "",
            "Morning Nautical Twilight": "(-12 degrees)",
            "Evening Nautical Twilight": "(-12 degrees)",
            "Morning Civil Twilight": "(-6 degrees)",
            "Evening Civil Twilight": "(-6 degrees)",
            "Sun Set": "",
            "Sun Rise": "",
        }
        return data_dict, help_dict, local_dict
