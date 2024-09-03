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


############################################
# Python Standard Library
from urllib.parse import urlencode
import itertools
from datetime import datetime
import warnings
from collections import defaultdict
############################################
# External Packages
import requests


MAX_CONNECT_TIMEOUT = 3.1    # seconds
MAX_READ_TIMEOUT = 90 * 60   # seconds

class ApiAdapter:
    def __init__(self, *,
                 server_url='https://tucson-teststand.lsst.codes',
                 connect_timeout=3.05,  # seconds
                 read_timeout=10 * 60,  # seconds
                 ):
        self.server = server_url
        self.c_timeout = min(MAX_CONNECT_TIMEOUT,
                             float(connect_timeout))  # seconds
        self.r_timeout = min(MAX_READ_TIMEOUT,  # seconds
                             float(read_timeout))
        self.timeout = (self.c_timeout, self.r_timeout)


class NarrativelogAdapter(ApiAdapter):
    service = 'narrativelog'

    def get_messages(self,
                     site_ids=None,
                     message_text=None,
                     min_date_end=None,
                     max_date_end=None,
                     is_human='either',
                     is_valid='either',
                     offset=None,
                     limit=None
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
        except Exception as err:
            warnings.warn(f'No {self.service} records retrieved: {err}')
            recs = []
        if len(recs) == 0:
            raise Exception(f'No records retrieved from {url}')

        self.recs = recs
        self.recs.sort(key=lambda r: r['date_begin'])
        return recs

    def get_timelost(self, rollup='day'):
        day_tl = dict() # day_tl[day] = totalDayTimeLost
        for day,dayrecs in itertools.groupby(
                self.recs,
                key=lambda r: datetime.fromisoformat(r['date_begin']).date().isoformat()
                ):
            day_tl[day] = sum([r['time_lost'] for r in dayrecs])
        return day_tl

class ExposurelogAdapter(ApiAdapter):
    service = 'exposurelog'

    def get_instruments(self):
        url = f'{self.server}/{self.service}/instruments'
        try:
            instruments = requests.get(url, timeout=self.timeout).json()
        except Exception as err:
            warnings.warn(f'No instruments retrieved: {err}')
            instruments = dict(dummy=[])
        # Flatten the lists
        return list(itertools.chain.from_iterable(instruments.values()))

    def get_exposures(self, instrument, registry=1):
        qparams = dict(instrument=instrument, registery=registry)
        url = f'{self.server}/{self.service}/exposures?{urlencode(qparams)}'
        try:
            recs = requests.get(url, timeout=self.timeout).json()
        except Exception as err:
            warnings.warn(f'No exposures retrieved: {err}')
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
                     limit=None
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
        try:
            recs = requests.get(url, timeout=self.timeout).json()
        except Exception as err:
            warnings.warn(f'No {self.service} records retrieved: {err}')
            recs = []
        if len(recs) == 0:
            raise Exception(f'No records retrieved from {url}')

        self.recs = recs
        self.recs.sort(key=lambda r: r['day_obs'])
        return recs

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



# gaps,recs = logrep_utils.ExposurelogAdapter(server_url='https://usdf-rsp-dev.slac.stanford.edu').get_observation_gaps('LSSTComCam')

# gaps,recs = logrep_utils.ExposurelogAdapter(server_url='[[https://tucson-teststand.lsst.codes').get_observation_gaps('LSSTComCam')
