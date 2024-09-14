from datetime import datetime, date, time, timedelta
import math
from astroplan import Observer
from astropy.time import Time

class Almanac:

    def __init__(self, *, day_obs=None, site='Rubin'):
        if day_obs is None:
            astro_day = date.today() - timedelta(days=1)
        else:
            astro_day = datetime.strptime(str(day_obs), '%Y%m%d').date()

        self.observer = Observer.at_site(site, timezone='Chile/Continental')
        self.astro_day = astro_day
        self.astro_noon = datetime.combine(self.astro_day,time(12))

        self.get_moon()
        self.get_sun()

    def get_moon(self):
        self.moon_rise_time = self.observer.moon_rise_time(self.astro_noon)
        self.moon_set_time = self.observer.moon_set_time(self.astro_noon)

        # Percent of moon lit
        self.moon_illum = self.observer.moon_illumination(self.astro_noon)

    def get_sun(self):
        time = self.observer.datetime_to_astropy_time(self.astro_noon)

        # ast(ronoimical) twilight: -18 degrees)
        self.ast_twilight_morning = self.observer.twilight_morning_astronomical(
            time)
        self.ast_twilight_evening = self.observer.twilight_evening_astronomical(
            time)

        # nau(tical) twilight: -12 degrees)
        self.nau_twilight_morning = self.observer.twilight_morning_nautical(
            time)
        self.nau_twilight_evening = self.observer.twilight_evening_nautical(
            time)

        # civ(il) twilight: -6 degrees)
        self.civ_twilight_morning = self.observer.twilight_morning_civil(
            time)
        self.civ_twilight_evening = self.observer.twilight_evening_civil(
            time)

        self.sun_rise_time = self.observer.sun_rise_time(time)
        self.sun_set_time = self.observer.sun_set_time(time)

    @property
    def as_dict(self):
        data_dict =  {
            'Moon Rise': self.moon_rise_time.iso,
            'Moon Set': self.moon_set_time.iso,
            'Moon Illumination': f'{self.moon_illum:.0%}',
            'Astronomical Twilight (morning)': self.ast_twilight_morning.iso,
            'Astronomical Twilight (evening)': self.ast_twilight_evening.iso,
            'Nautical Twilight (morning)': self.nau_twilight_morning.iso,
            'Nautical Twilight (evening)': self.nau_twilight_evening.iso,
            'Civil Twilight (morning)': self.civ_twilight_morning.iso,
            'Civil Twilight (evening)': self.civ_twilight_evening.iso,
            }
        help_dict = {
            'Moon Rise': '',
            'Moon Set': '',
            'Moon Illumination': '(% lit)',
            'Astronomical Twilight (morning)': '(-18 degrees)',
            'Astronomical Twilight (evening)': '(-18 degrees)',
            'Nautical Twilight (morning)': '(-12 degrees)',
            'Nautical Twilight (evening)': '(-12 degrees)',
            'Civil Twilight (morning)': '(-6 degrees)',
            'Civil Twilight (evening)': '(-6 degrees)',
            }
        return data_dict, help_dict
