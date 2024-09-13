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


'''\
TODO: This is considered Proof of Concept  code.
Tests and documentation exist minimally or not at all since until the
concept is Proven, it all might be thrown away or rewritten.
'''


############################################
# Python Standard Library
from urllib.parse import urlencode
import itertools
from datetime import datetime, date, time, timedelta
from warnings import warn
from collections import defaultdict
from abc import ABC

############################################
# External Packages
import requests

MAX_CONNECT_TIMEOUT = 3.1   # seconds
MAX_READ_TIMEOUT = 180      # seconds


class SourceAdapter(ABC):
    """Abstract Base Class for all source adapters.
    """
    # TODO document class including all class variables.
    def __init__(self, *,
                 server_url='https://tucson-teststand.lsst.codes',
                 connect_timeout=1.05,  # seconds
                 read_timeout=2,  # seconds
                 ):
        self.server = server_url
        self.c_timeout = min(MAX_CONNECT_TIMEOUT,
                             float(connect_timeout))  # seconds
        self.r_timeout = min(MAX_READ_TIMEOUT,  # seconds
                             float(read_timeout))
        self.timeout = (self.c_timeout, self.r_timeout)

        # Provide the following in subclass
        output_fields = None
        service = None
        endpoints = None

    def keep_fields(self, recs, outfields):
        """Keep only keys in OUTFIELDS list of RECS (list of dicts)
        SIDE EFFECT: Removes extraneous keys from all dicts in RECS.
        """
        if outfields:
            for rec in recs:
                nukefields = set(rec.keys()) - set(outfields)
                for f in nukefields:
                    del rec[f]

    @property
    def row_header(self):
        return '| Time | Message |\n|--------|------|'

    def row_str_func(self, datetime_str, rec):
        return f"{datetime_str} | {rec['message_text']}"

    def day_table(self, recs, datetime_field,
                  time_only=None,
                  is_dayobs=False,
                  row_str_func=None,
                  ):
        def date_time(rec):
            if is_dayobs:
                dt = datetime.strptime(str(rec[datetime_field]), '%Y%m%d')
            else:
                dt = datetime.fromisoformat(rec[datetime_field])
            return dt.replace(microsecond=0)

        if len(recs) == 0:
            print('Nothing to display.')
            return
        dates = set([date_time(r).date() for r in recs])
        if time_only is None:
            time_only = True if len(dates) == 1 else False
        tablestr = ''
        for rec in recs:
            dt = date_time(rec)
            dtstr = str(dt.time()) if time_only else str(dt)
            tablestr += f'\n{self.row_str_func(dtstr, rec)}'
        return tablestr + ":EOT"

    @property
    def source_url(self):
        return f'{self.server}/{self.service}'

    def check_endpoints(self, timeout=None, verbose=True):
        to = (timeout or self.timeout)
        if verbose:
            print(f'Try connect to each endpoint of'
                  f' {self.server}/{self.service} ')
        url_http_status_code = dict()
        for ep in self.endpoints:
            url = f'{self.server}/{self.service}/{ep}'
            try:
                r = requests.get(url, timeout=(timeout or self.timeout))
            except Exception as err:
                url_http_status_code[url] = 'GET error'
            else:
                url_http_status_code[url] = r.status_code
        return url_http_status_code


    def analytics(self, recs, categorical_fields=None):
        if len(recs) == 0:
            return dict(fields=[],
                        facet_fields=set(),
                        facets=dict())

        non_cats = set([
            'tags', 'urls', 'message_text', 'id', 'date_added',
            'obs_id', 'day_obs', 'seq_num', 'parent_id', 'user_id',
            'date_invalidated', 'date_begin', 'date_end',
            'time_lost', # float
            # 'systems','subsystems','cscs',  # values need special handling
        ])
        flds = set(recs[0].keys())
        if not categorical_fields:
            categorical_fields = flds
        ignore_fields = flds - categorical_fields
        facflds = flds - ignore_fields

        # facets(field) = set(value-1, value-2, ...)
        facets = {fld: set([str(r[fld])
                    for r in recs if not isinstance(r[fld], list)])
                    for fld in facflds}
        return dict(fields=flds,
                    facet_fields=facflds,
                    facets=facets,
                    )
# END: class SourceAdapter


# Not available on SLAC (usdf) as of 9/9/2024.
class NightReportAdapter(SourceAdapter):
    service = "nightreport"
    endpoints = ['reports']
    primary_endpoint = 'reports'

class NarrativelogAdapter(SourceAdapter):
    """TODO full documentation
    """
    service = 'narrativelog'
    endpoints = ['messages',]
    primary_endpoint = 'messages'
    outfields = {
        # 'category',
        # 'components',
        # 'cscs',
        'date_added',
        # 'date_begin',
        # 'date_end',
        # 'date_invalidated',
        # 'id',
        # 'is_human',
        # 'is_valid',
        # 'level',
        'message_text',
        # 'parent_id',
        # 'primary_hardware_components',
        # 'primary_software_components',
        # 'site_id',
        # 'subsystems',
        # 'systems',
        # 'tags',
        'time_lost',
        'time_lost_type',
        # 'urls',
        # 'user_agent',
        # 'user_id',
    }

    def get_messages(self,
                     site_ids=None,
                     message_text=None,
                     min_date_end=None,
                     max_date_end=None,
                     is_human='either',
                     is_valid='either',
                     offset=None,
                     limit=None,
                     ):
        qparams = dict(is_human=is_human, is_valid=is_valid)
        if site_ids:
            qparams['site_ids'] = site_ids
        if message_text:
            qparams['message_text'] = message_text
        if min_date_end:
            qparams['min_date_end'] = min_date_end
        if max_date_end:
            qparams['max_date_end'] = max_date_end
        if limit:
            qparams['limit'] = limit

        qstr = urlencode(qparams)
        url = f'{self.server}/{self.service}/messages?{qstr}'
        try:
            recs = requests.get(url, timeout=self.timeout).json()
            recs.sort(key=lambda r: r['date_begin'])
        except Exception as err:
            warn(f'No {self.service} records retrieved: {err}')
            recs = []

        self.keep_fields(recs, self.outfields)
        return recs,url

    def get_timelost(self, recs, rollup='day'):
        def iso_date_begin(rec):
            return datetime.fromisoformat(rec['date_begin']).date().isoformat()

        day_tl = dict() # day_tl[day] = totalDayTimeLost
        for day,dayrecs in itertools.groupby(recs, key=iso_date_begin):
            day_tl[day] = sum([r['time_lost'] for r in dayrecs])
        return day_tl
# END: class NarrativelogAdapter

class ExposurelogAdapter(SourceAdapter):
    """TODO full documentation

    EXAMPLES:
       gaps,recs = logrep_utils.ExposurelogAdapter(
             server_url='https://usdf-rsp-dev.slac.stanford.edu'
             ).get_observation_gaps('LSSTComCam')
       gaps,recs = logrep_utils.ExposurelogAdapter(
             server_url='[[https://tucson-teststand.lsst.codes'
             ).get_observation_gaps('LSSTComCam')
    """
    ignore_fields = ['id']
    service = 'exposurelog'
    endpoints = [
        'instruments',
        'exposures',
        'messages',
    ]
    primary_endpoint = 'messages'
    outfields = {
        'date_added',
        # 'date_invalidated',
        # 'day_obs',
        # 'exposure_flag',
        # 'id',
        # 'instrument',
        # 'is_human',
        # 'is_valid',
        # 'level',
        'message_text',
        'obs_id',
        # 'parent_id',
        # 'seq_num',
        # 'site_id',
        # 'tags',
        # 'urls',
        # 'user_agent',
        # 'user_id',
    }

    @property
    def row_header(self):
        return '| Time | OBS ID | Message |\n|--------|-------|------|'

    def row_str_func(self, datetime_str, rec):
        return f"{datetime_str} | {rec['obs_id']} | {rec['message_text']}"

    def check_endpoints(self, timeout=None, verbose=True):
        to = (timeout or self.timeout)
        if verbose:
            print(f'Try connect to each endpoint of '
                  f'{self.server}/{self.service} ')
        url_http_status_code = dict()

        for ep in self.endpoints:
            qstr = '?instrument=na' if ep == 'exposures' else ''
            url = f'{self.server}/{self.service}/{ep}{qstr}'
            try:
                r = requests.get(url, timeout=to)
            except Exception as err:
                url_http_status_code[url] = 'GET error'
            else:
                url_http_status_code[url] = r.status_code
        return url_http_status_code


    def get_instruments(self):
        url = f'{self.server}/{self.service}/instruments'
        try:
            instruments = requests.get(url, timeout=self.timeout).json()
        except Exception as err:
            warn(f'No instruments retrieved: {err}')
            instruments = dict(dummy=[])
        # Flatten the lists
        return list(itertools.chain.from_iterable(instruments.values()))

    def get_exposures(self, instrument, registry=1):
        qparams = dict(instrument=instrument, registery=registry)
        url = f'{self.server}/{self.service}/exposures?{urlencode(qparams)}'
        try:
            recs = requests.get(url, timeout=self.timeout).json()
        except Exception as err:
            warn(f'No exposures retrieved: {err}')
            recs = []
        return recs

    def get_messages(self,
                     site_ids=None,
                     obs_ids=None,
                     instruments=None,
                     message_text=None,
                     min_day_obs=None,
                     max_day_obs=None,
                     is_human='either',
                     is_valid='either',
                     exposure_flags=None,
                     offset=None,
                     limit=None,
                     ):
        qparams = dict(is_human=is_human, is_valid=is_valid)
        if site_ids:
            qparams['site_ids'] = site_ids
        if obs_ids:
            qparams['obs_ids'] = obs_ids
        if instruments:
            qparams['instruments'] = instruments
        if min_day_obs:
            qparams['min_day_obs'] = min_day_obs
        if max_day_obs:
           qparams['max_day_obs'] = max_day_obs
        if exposure_flags:
            qparams['exposure_flags'] = exposure_flags
        if offset:
            qparams['offset'] = offset
        if limit:
            qparams['limit'] = limit

        qstr = urlencode(qparams)
        url = f'{self.server}/{self.service}/messages?{qstr}'
        recs = []
        try:
            response = requests.get(url, timeout=self.timeout)
            recs = response.json()
        except Exception as err:
            warn(f'No {self.service} records retrieved: {err}')

        if len(recs) == 0:
            warn(f'No records retrieved from {url}')

        if recs:
            recs.sort(key=lambda r: r['day_obs'])

        self.keep_fields(recs, self.outfields)
        return recs,url

    def get_observation_gaps(self, instruments=None,
                             min_day_obs=None,  # YYYYMMDD
                             max_day_obs=None,  # YYYYMMDD
                             ):
        if not instruments:
            instruments = self.get_instruments()
        assert isinstance(instruments,list), \
            f'"instruments" must be a list.  Got {instruments!r}'
        # inst_day_rollupol[instrument] => dict[day] => exposureGapInMinutes
        inst_day_rollup = defaultdict(dict)  # Instrument/Day rollup

        for instrum in instruments:
            recs = self.get_exposures(instrum)
            instrum_gaps = dict()
            for day,dayrecs in itertools.groupby(recs,
                                                 key=lambda r: r['day_obs']):
                gaps = list()
                begin = end = None
                for rec in dayrecs:
                    begin = rec['timespan_begin']
                    if end:
                        # span in minutes
                        diff = (datetime.fromisoformat(begin)
                                - datetime.fromisoformat(end)
                                ).total_seconds() / 60.0

                        gaps.append((
                            datetime.fromisoformat(end).time().isoformat(),
                            datetime.fromisoformat(begin).time().isoformat(),
                            diff
                        ))
                    end = rec['timespan_end']
                instrum_gaps[day] = gaps

                #!roll = dict()
                # Rollup gap times by day
                for day,tuples in instrum_gaps.items():
                    #!roll[day] = sum([t[2] for t in tuples])
                    inst_day_rollup[instrum][day] = sum([t[2] for t in tuples])

        return inst_day_rollup
# END: class ExposurelogAdapter



class Dashboard:  # TODO Move to its own file (utils.py).
    """Verify that we can get to all the API endpoints and databases we need for
    any of our sources.
    """

    envs = dict(
        summit = 'https://summit-lsp.lsst.codes',
        usdf_dev = 'https://usdf-rsp-dev.slac.stanford.edu',
        tucson = 'https://tucson-teststand.lsst.codes',
        # Environments not currently used:
        #    rubin_usdf_dev = '',
        #    data_lsst_cloud = '',
        #    usdf = '',
        #    base_data_facility = '',
        #    rubin_idf_int = '',
    )
    adapters = [ExposurelogAdapter,
                NarrativelogAdapter,
                # NightReportAdapter,   # TODO
                ]

    def report(self, timeout=None):
        """Check our ability to connect to every Source on every Environment.
        Report a summary.

        RETURN: percentage of good connectons.
        """
        url_status = dict()
        for env,server in self.envs.items():
            for adapter in self.adapters:
                service = adapter(server_url=server)
                # url_status[endpoint_url] = http_status_code
                url_status.update(service.check_endpoints(timeout=timeout))

        total_cnt = good_cnt = 0
        good = list()
        bad = list()
        for url,stat in url_status.items():
            total_cnt += 1
            if stat == 200:
                good_cnt += 1
                good.append(url)
            else:
                bad.append((url,stat))

        print(f'\nConnected to {good_cnt} out of {total_cnt} endpoints.'
              f'({good_cnt/total_cnt:.0%})'
              )
        goodstr = "\n\t".join(good)
        print(f'Successful connects ({good_cnt}): ')
        for gurl in good:
            print(f'\t{gurl}')

        print(f'Failed connects ({total_cnt - good_cnt}): ')
        for burl,stat in bad:
            print(f'\t{stat}: {burl}')

        status = dict(num_good=good_cnt,
                      num_total=total_cnt,
                      good_urls=good,
                      bad_ursl=bad,
                      )
        return good_cnt/total_cnt
# END: class Dashboard

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
        self.get_twilight()

    def get_moon(self):
        self.moon_rise_time = self.observer.moon_rise_time(self.astro_noon)
        self.moon_set_time = self.observer.moon_set_time(self.astro_noon)

    @property
    def moon_rise(self):
        return self.moon_rise_time

    @property
    def moon_set(self):
        return self.moon_set_time

    def get_twilight(self):
        self.twilight_morning = self.observer.twilight_morning_astronomical(
            self.observer.datetime_to_astropy_time(self.astro_noon))
        self.twilight_evening = self.observer.twilight_evening_astronomical(
            self.observer.datetime_to_astropy_time(self.astro_noon))
