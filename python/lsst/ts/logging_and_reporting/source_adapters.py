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

# Python Standard Library
from urllib.parse import urlencode
import itertools
from datetime import datetime, date, time, timedelta
from warnings import warn
from collections import defaultdict
from abc import ABC
# External Packages
import requests
# Local Packages
import lsst.ts.logging_and_reporting.utils as ut
import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.almanac as alm



MAX_CONNECT_TIMEOUT = 3.1   # seconds
MAX_READ_TIMEOUT = 180      # seconds
summit = 'https://summit-lsp.lsst.codes'
usdf = 'https://usdf-rsp-dev.slac.stanford.edu'
tucson = 'https://tucson-teststand.lsst.codes'

default_server = usdf

def all_endpoints(server):
    endpoints = itertools.chain.from_iterable(
        [sa(server_url=server).used_endpoints()  for sa in adapters]
        )
    return list(endpoints)


def validate_response(response, endpoint_url):
    if response.status_code == 200:
        return True
    else:
        msg = f'Error: {response.json()} {endpoint_url=} {response.reason}'
        raise ex.BadStatus(msg)


class SourceAdapter(ABC):
    """Abstract Base Class for all source adapters.
    """
    limit=99

    # TODO document class including all class variables.
    def __init__(self, *,
                 server_url=None,
                 min_day_obs=None,  # INCLUSIVE: default=Yesterday
                 max_day_obs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
                 offset=0,
                 connect_timeout=1.05,  # seconds
                 read_timeout=2,  # seconds
                 ):
        """Load the relevant data for the Source.

        Intended to load from all used Source endpoints over the range
        of day_obs specified in INIT. The day(s) records for each
        endpoint is stored for later use.  Do not make the day_obs
        range large or you will use lots of memory. Tens of days is probably
        ok.
        """
        if min_day_obs is None:  # Inclusive
            min_day_obs = ut.datetime_to_day_obs(
                datetime.today() - timedelta(days=1))
        if max_day_obs is None:  # Exclusive
            max_day_obs = ut.datetime_to_day_obs(
                datetime.today() + timedelta(days=1))
        self.server = server_url or default_server
        self.min_day_obs = min_day_obs
        self.max_day_obs = max_day_obs
        self.min_date = ut.get_datetime_from_day_obs_str(min_day_obs)
        self.max_date = ut.get_datetime_from_day_obs_str(max_day_obs)
        self.offset = offset
        self.c_timeout = min(MAX_CONNECT_TIMEOUT,
                             float(connect_timeout))  # seconds
        self.r_timeout = min(MAX_READ_TIMEOUT,  # seconds
                             float(read_timeout))
        self.timeout = (self.c_timeout, self.r_timeout)

        self.records = None  # else: list of dict
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
        msg = rec['message_text']
        return f'> {datetime_str} | <pre><code>{msg}</code></pre>'


    # Break on DAY_OBS. Within that, break on DATE, within that only show time.
    def day_table(self, datetime_field,
                  dayobs_field=None,
                  row_str_func=None,
                  zero_message=False,
                  ):
        def obs_night(rec):
            if 'day_obs' in rec:
                return ut.day_obs_str(rec['day_obs']) # -> # "YYYY-MM-DD"
            else:
                dt = datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_day_obs(dt)

        def obs_date(rec):
            dt = datetime.fromisoformat(rec[datetime_field])
            return dt.replace(microsecond=0)

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print('Nothing to display.')
            return
        dates = set([obs_date(r).date() for r in recs])
        table = list()
        # Group by night.
        recs = sorted(recs,key=lambda r: obs_night(r))
        for night,g0 in itertools.groupby(recs, key=lambda r: obs_night(r)):
            # Group by date
            table.append(f'## NIGHT: {night}: ')
            for date,g1 in itertools.groupby(g0, key=lambda r: obs_date(r)):
                table.append(f'### DATE: {date.date()}: ')
                for rec in g0:
                    dt = obs_date(rec)
                    dtstr = str(dt.time())
                    table.append(f'{self.row_str_func(dtstr, rec)}')
        table.append(':EOT')
        return table


    @property
    def source_url(self):
        return f'{self.server}/{self.service}'

    def used_endpoints(self):
        used = list()
        for ep in self.endpoints:
            used.append(f'{self.server}/{self.service}/{ep}')
        return used


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
                validate_response(r, url)
            except Exception as err:
                url_http_status_code[url] = 'GET error'
            else:
                url_http_status_code[url] = r.status_code
        return url_http_status_code, all([v==200 for v in url_http_status_code.values()])


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
    outfields = {
        'confluence_url',
        'date_added',
        'date_invalidated',
        'date_sent',
        'day_obs',
        'id',
        'is_valid',
        'observers_crew',
        'parent_id',
        'site_id',
        'summary',
        'telescope',
        'telescope_status',
        'user_agent',
        'user_id',
        }

    def __init__(self, *,
                 server_url=None,
                 min_day_obs=None,  # INCLUSIVE: default=Yesterday
                 max_day_obs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
                 limit=None,
                 ):
        super().__init__()
        self.limit = SourceAdapter.limit if limit is None else limit
        self.server = SourceAdapter.server if server_url is None else server_url

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        self.status['reports'] = self.get_reports()

    def row_str_func(self, datetime_str, rec):
        return f"> {datetime_str} | <pre>{rec['summary']}</pre>"

    def get_reports(self,
                    site_ids=None,
                    summary=None,
                    is_human='either',
                    is_valid='true',
                    ):
        qparams = dict(is_human=is_human, is_valid=is_valid)
        if site_ids:
            qparams['site_ids'] = site_ids
        if summary:
            qparams['summary'] = summary
        if self.min_day_obs:
            qparams['min_day_obs'] = ut.day_obs_int(self.min_day_obs)
        if self.max_day_obs:
            qparams['max_day_obs'] = ut.day_obs_int(self.max_day_obs)
        if self.limit:
            qparams['limit'] = self.limit

        qstr = urlencode(qparams)
        url = f'{self.server}/{self.service}/reports?{qstr}'
        error = None
        try:
            response = requests.get(url, timeout=self.timeout)
            validate_response(response, url)
            recs = response.json()
            recs.sort(key=lambda r: r['day_obs'])
        except Exception as err:
            recs = []
            error = f'{response.text=} Exception={err}'

        self.keep_fields(recs, self.outfields)
        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
            )
        return status

    def nightly_tickets(self, recs):
        tickets = defaultdict(set)  # tickets[day_obs] = {ticket_url, ...}
        for r in recs:
            ticket_url = r['confluence_url']
            if ticket_url:
                tickets[r['day_obs']].add(ticket_url)
        return {dayobs:list(urls) for dayobs,urls in tickets.items()}


class NarrativelogAdapter(SourceAdapter):
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

    def __init__(self, *,
                 server_url=None,
                 min_day_obs=None,  # INCLUSIVE: default=Yesterday
                 max_day_obs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
                 limit=None,
                 ):
        super().__init__()
        self.limit = SourceAdapter.limit if limit is None else limit
        self.server = SourceAdapter.server if server_url is None else server_url

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        self.status['messages'] = self.get_messages()

    def get_messages(self,
                     site_ids=None,
                     message_text=None,
                     is_human='either',
                     is_valid='true',
                     offset=None,
                     ):
        qparams = dict(
            is_human=is_human,
            is_valid=is_valid,
            order_by='-date_begin',
        )
        if site_ids:
            qparams['site_ids'] = site_ids
        if message_text:
            qparams['message_text'] = message_text
        if self.min_day_obs:
            qparams['min_date_added'] = datetime.combine(
                self.min_date, time()
            ).isoformat()
        if self.max_day_obs:
            qparams['max_date_added'] = datetime.combine(
                self.max_date, time()
            ).isoformat()
        if self.limit:
            qparams['limit'] = self.limit

        qstr = urlencode(qparams)
        url = f'{self.server}/{self.service}/messages?{qstr}'
        error = None
        try:
            r = requests.get(url, timeout=self.timeout)
            validate_response(r, url)
            recs = r.json()
            recs.sort(key=lambda r: r['date_begin'])
        except Exception as err:
            recs = []
            error = str(err)

        self.keep_fields(recs, self.outfields)
        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
            )
        return status

    def get_timelost(self, recs, rollup='day'):
        def iso_date_begin(rec):
            return datetime.fromisoformat(rec['date_begin']).date().isoformat()

        day_tl = dict() # day_tl[day] = totalDayTimeLost
        for day,dayrecs in itertools.groupby(recs, key=iso_date_begin):
            day_tl[day] = sum([r['time_lost'] for r in dayrecs])
        return day_tl
# END: class NarrativelogAdapter


class ExposurelogAdapter(SourceAdapter):
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
        'day_obs',
        # 'exposure_flag',
        # 'id',
        'instrument',
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

    def __init__(self, *,
                 server_url='https://tucson-teststand.lsst.codes',
                 min_day_obs=None,  # INCLUSIVE: default=Yesterday
                 max_day_obs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
                 limit=None,
                 ):
        super().__init__()
        self.limit = SourceAdapter.limit if limit is None else limit
        self.server = SourceAdapter.server if server_url is None else server_url

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()
        self.instruments = list() # [instrument, ...]
        self.exposures = dict() # dd[instrument] = [rec, ...]

        # Load the data (records) we need from relevant endpoints
        # in dependency order.
        self.status['instruments'] = self.get_instruments()
        for instrument in self.instruments:
            self.status[f'exposures.{instrument}'] = self.get_exposures(instrument)
        self.status['messages'] = self.get_messages()


    @property
    def row_header(self):
        return('| Time | OBS ID | Telescope | Message |\n'
               '|------|--------|-----------|---------|'
               )

    def row_str_func(self, datetime_str, rec):
        return(f"> {datetime_str} "
               f"| {rec['obs_id']} "
               f"| {rec['instrument']} "
               f"| <pre>{rec['message_text']}</pre>"
               )


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
                validate_response(r, url)
            except Exception as err:
                url_http_status_code[url] = 'GET error'
            else:
                url_http_status_code[url] = r.status_code
        allgood_p = all([v==200 for v in url_http_status_code.values()])
        return url_http_status_code, allgood_p

    def get_instruments(self):
        url = f'{self.server}/{self.service}/instruments'
        recs = dict(dummy=[])
        error = None
        try:
            r = requests.get(url, timeout=self.timeout).json()
            validate_response(r, url)
            recs = r.json()
        except Exception as err:
            error = str(err)

        # Flatten the lists
        self.instruments = list(itertools.chain.from_iterable(recs.values()))
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
            )
        return status

    def get_exposures(self, instrument, registry=1):
        qparams = dict(instrument=instrument, registery=registry)
        if self.min_day_obs:
            qparams['min_day_obs'] = ut.day_obs_int(self.min_day_obs)
        if self.max_day_obs:
            qparams['max_day_obs'] = ut.day_obs_int(self.max_day_obs)
        url = f'{self.server}/{self.service}/exposures?{urlencode(qparams)}'
        recs = []
        error = None
        try:
            recs = requests.get(url, timeout=self.timeout).json()
        except Exception as err:
            error = str(err)

        self.exposures[instrument] = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
            )
        return status



    def get_messages(self,
                     site_ids=None,
                     obs_ids=None,
                     instruments=None,
                     message_text=None,
                     is_human='either',
                     is_valid='true',
                     exposure_flags=None,
                     ):
        qparams = dict(is_human=is_human,
                       is_valid=is_valid,
                       order_by='-date_added',
                       )
        if site_ids:
            qparams['site_ids'] = site_ids
        if obs_ids:
            qparams['obs_ids'] = obs_ids
        if instruments:
            qparams['instruments'] = instruments
        if self.min_day_obs:
            qparams['min_day_obs'] = ut.day_obs_int(self.min_day_obs)
        if self.max_day_obs:
            qparams['max_day_obs'] = ut.day_obs_int(self.max_day_obs)
        if exposure_flags:
            qparams['exposure_flags'] = exposure_flags
        if self.limit:
            qparams['limit'] = self.limit

        qstr = urlencode(qparams)
        url = f'{self.server}/{self.service}/messages?{qstr}'
        recs = []
        error = None
        try:
            response = requests.get(url, timeout=self.timeout)
            validate_response(response, url)
            recs = response.json()
        except Exception as err:
            error = str(err)

        if recs:
            recs.sort(key=lambda r: r['day_obs'])

        self.keep_fields(recs, self.outfields)
        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
            )
        return status

    # day_obs:: YYYMMDD (int or str)
    # Use almanac begin of night values for day_obs.
    # Use almanac end of night values for day_obs + 1.
    def night_tally_observation_gaps(self, day_obs,
                                     instrument='LSSTComCam'):
        almanac = alm.Almanac(day_obs=day_obs)
        total_observable_hours = almanac.night_hours
        recs = self.get_night_exposures(instrument, day_obs)


    def get_observation_gaps(self, instruments=None):
        if not instruments:
            instruments = self.instruments
        # TODO user specified list of instruments must be subset
        assert isinstance(instruments,list), \
            f'"instruments" must be a list.  Got {instruments!r}'
        # inst_day_rollup[instrument] => dict[day] => exposureGapInMinutes
        inst_day_rollup = defaultdict(dict)  # Instrument/Day rollup
        for instrum in instruments:
            recs = self.exposures[instrum]
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

                # Rollup gap times by day
                for day,tuples in instrum_gaps.items():
                    inst_day_rollup[instrum][day] = sum([t[2] for t in tuples])

        return inst_day_rollup
# END: class ExposurelogAdapter

adapters = [ExposurelogAdapter,
            NarrativelogAdapter,
            NightReportAdapter,
            ]
