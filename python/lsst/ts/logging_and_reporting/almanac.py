import datetime as dt
import warnings

import astropy.coordinates
import pandas as pd
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
        dome_tz = pytz.timezone("Chile/Continental")
        self.dome_noon = Time(dome_tz.localize(dt.datetime.strptime(dobs + " 12:00", "%Y%m%d %H:%M")))

        astro_day = dt.datetime.strptime(dobs, "%Y%m%d").date()
        astro_date = dt.datetime.strptime(dobs, "%Y%m%d")

        dome_tz = pytz.timezone("Chile/Continental")

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

    @property
    def sources(self):
        return {
            "Astroplan": ("https://astroplan.readthedocs.io/en/stable/api/astroplan.Observer.html"),
        }

    def get_moon(self):
        self.moon_rise_time = self.observer.moon_rise_time(self.astro_midnight, which="nearest")
        self.moon_set_time = self.observer.moon_set_time(self.astro_midnight, which="nearest")

        # Percent of moon lit
        self.moon_illum = self.observer.moon_illumination(self.astro_midnight)

    def get_sun(self):
        # ast(ronoimical) twilight: -18 degrees)
        obs = self.observer
        self.ast_twilight_morning = obs.twilight_morning_astronomical(self.astro_midnight, which="next")
        self.ast_twilight_evening = obs.twilight_evening_astronomical(self.astro_midnight, which="previous")

        # nau(tical) twilight: -12 degrees)
        self.nau_twilight_morning = self.observer.twilight_morning_nautical(self.astro_midnight, which="next")
        self.nau_twilight_evening = self.observer.twilight_evening_nautical(
            self.astro_midnight, which="previous"
        )

        # civ(il) twilight: -6 degrees)
        self.civ_twilight_morning = self.observer.twilight_morning_civil(self.astro_midnight, which="next")
        self.civ_twilight_evening = self.observer.twilight_evening_civil(
            self.astro_midnight, which="previous"
        )

        self.sun_rise_time = self.observer.sun_rise_time(self.astro_midnight, which="next")
        self.sun_set_time = self.observer.sun_set_time(self.astro_midnight, which="previous")

    @property
    def night_hours(self):
        day_delta = self.nau_twilight_morning - self.nau_twilight_evening
        return day_delta.to_value("hr")

    def events(self, localize=False, iso=False):
        """Sun/Moon datetime in UTC. Use localize=True for Chile time."""
        events = dict(  # as astropy.Time
            moon_rise=self.moon_rise_time,
            moon_set=self.moon_set_time,
            sunrise_18deg=self.ast_twilight_morning,
            sunset_18deg=self.ast_twilight_evening,
            solar_midnight=self.astro_midnight,
            sunrise_12deg=self.nau_twilight_morning,
            sunset_12deg=self.nau_twilight_evening,
            sunrise_6deg=self.civ_twilight_morning,
            sunset_6deg=self.civ_twilight_evening,
            sunrise=self.sun_rise_time,
            sunset=self.sun_set_time,
            dome_noon=self.dome_noon,
        )

        if localize:
            events_dt = {k: self.observer.astropy_time_to_datetime(v) for k, v in events.items()}
        else:
            events_dt = {k: v.to_datetime(leap_second_strict="silent") for k, v in events.items()}

        if iso:
            return {k: v.isoformat(sep=" ", timespec="seconds") for k, v in events_dt.items()}
        else:
            return events_dt

    @property
    def dataframe(self):
        df = pd.DataFrame(
            [
                self.events(localize=True, iso=True),
                self.events(localize=False, iso=True),
            ]
        ).T
        df.columns = ["Chile/Continental", "UTC"]
        df.index.name = "Event"
        return df.sort_values(by="UTC").reset_index().set_index("UTC")

    @property
    def as_dict(self):
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

        # Maybe it we should add a column of times for the Dome.
        # It would make it easier to do some kinds of sanity checks.
        # Then again, it might confuse the issues.
        # It depends on who will be looking this the most.
        # Observers in the Dome? People elsewhere?

        data_dict = {
            "": "UTC",
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
        return data_dict, help_dict

    # A time_log is a DF ordered and indexed with DatetimeIndex.
    def as_records(self):
        """Sun/Moon events indexed by UTC (ISO string truncated to seconds)"""
        return self.dataframe.reset_index().to_dict(orient="records")
