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


# TODO: This is considered Proof of Concept code.
# Tests and documentation exist minimally or not at all since until the
# concept is Proven, it all might be thrown away or rewritten.

# NOTE on day_obs vs dayobs:
# Throughout Rubin, and perhaps Astonomy in general, a single night
# of observering (both before and after midnight portions) is referred
# to using 'date_obs' or 'dateobs'.
# Generaly its used as a single word when refering to a TYPE
# and as two words when referring to a FIELD. But there are
# plenty exceptions.  Nonetheless, this is the convention we use.
# One word most of the time, two_words when its a field such as
# in a Database or API query string.

import itertools
from abc import ABC
from collections import defaultdict
from datetime import datetime, time, timedelta
from urllib.parse import urlencode

import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.utils as ut
import requests

MAX_CONNECT_TIMEOUT = 3.1  # seconds
MAX_READ_TIMEOUT = 180  # seconds
summit = "https://summit-lsp.lsst.codes"
usdf = "https://usdf-rsp-dev.slac.stanford.edu"
tucson = "https://tucson-teststand.lsst.codes"

default_server = usdf


def all_endpoints(server):
    endpoints = itertools.chain.from_iterable(
        [sa(server_url=server).used_endpoints() for sa in adapters]
    )
    return list(endpoints)


def validate_response(response, endpoint_url):
    if response.status_code == 200:
        return True
    else:
        msg = f"Error: {response.json()} {endpoint_url=} {response.reason}"
        raise ex.BadStatus(msg)


class SourceAdapter(ABC):
    """Abstract Base Class for all source adapters."""

    limit = 99

    # TODO document class including all class variables.
    def __init__(
        self,
        *,
        server_url=None,
        max_dayobs=None,  # EXCLUSIVE: default=TODAY other=YYYY-MM-DD
        min_dayobs=None,  # INCLUSIVE: default=max_dayobs - 1 day
        offset=0,
        connect_timeout=1.05,  # seconds
        read_timeout=2,  # seconds
    ):
        """Load the relevant data for the Source.

        Intended to load from all used Source endpoints over the range
        of dayobs specified in INIT. The day(s) records for each
        endpoint is stored for later use.  Do not make the dayobs
        range large or you will use lots of memory. Tens of days is probably
        ok.
        """

        self.server = server_url or default_server
        self.offset = offset
        cto = float(connect_timeout)
        self.c_timeout = min(MAX_CONNECT_TIMEOUT, cto)  # seconds
        self.r_timeout = min(MAX_READ_TIMEOUT, float(read_timeout))  # seconds
        self.timeout = (self.c_timeout, self.r_timeout)

        self.records = None  # else: list of dict

        # Provide the following in subclasses: self.service, self.endpoints

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        # e.g. status['messages'] = dict(endpoint_url='.../messages?...', ...)
        self.status = dict()

        # Store dayobs range
        self.max_date = ut.dayobs2dt(max_dayobs or "TODAY")
        self.max_dayobs = ut.datetime_to_dayobs(self.max_date)
        if min_dayobs:
            self.min_date = ut.dayobs2dt(min_dayobs)
        else:
            self.min_date = self.max_date - timedelta(days=1)
        assert self.min_date < self.max_date
        self.min_dayobs = ut.datetime_to_dayobs(self.min_date)

    def get_status(self, endpoint=None):
        return self.status.get(endpoint or self.primary_endpoint)

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
        return "| Time | Message |\n|--------|------|"

    def row_str_func(self, datetime_str, rec):
        msg = rec["message_text"].strip()
        return f"`{datetime_str}`\n```\n{msg}\n```"

    # ABC
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        row_str_func=None,
        zero_message=False,
    ):
        """Break on DAYOBS.
        Within that, break on DATE, within that only show time."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                dt = datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(dt)

        def obs_date(rec):
            dt = datetime.fromisoformat(rec[datetime_field])
            return dt.replace(microsecond=0)

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return
        table = list()
        # Group by night.
        recs = sorted(recs, key=lambda r: obs_night(r))
        for night, g0 in itertools.groupby(recs, key=lambda r: obs_night(r)):
            # Group by date
            table.append(f"## NIGHT: {night}: ")
            for date, g1 in itertools.groupby(g0, key=lambda r: obs_date(r)):
                table.append(f"### DATE: {date.date()}: ")
                for rec in g0:
                    dt = obs_date(rec)
                    dtstr = str(dt.time())
                    table.append(f"{self.row_str_func(dtstr, rec)}")
        table.append(":EOT")
        return table

    @property
    def source_url(self):
        return f"{self.server}/{self.service}"

    def used_endpoints(self):
        used = list()
        for ep in self.endpoints:
            used.append(f"{self.server}/{self.service}/{ep}")
        return used

    def check_endpoints(self, timeout=None, verbose=True):
        if verbose:
            msg = "Try connect to each endpoint of "
            msg += f"{self.server}/{self.service} "
            print(msg)

        url_http_status_code = dict()
        for ep in self.endpoints:
            url = f"{self.server}/{self.service}/{ep}"
            try:
                r = requests.get(url, timeout=(timeout or self.timeout))
                validate_response(r, url)
            except Exception:
                url_http_status_code[url] = "GET error"
            else:
                url_http_status_code[url] = r.status_code
        return url_http_status_code, all(
            [v == 200 for v in url_http_status_code.values()]
        )

    def analytics(self, recs, categorical_fields=None):
        if len(recs) == 0:
            return dict(fields=[], facet_fields=set(), facets=dict())

        flds = set(recs[0].keys())
        if not categorical_fields:
            categorical_fields = flds
        ignore_fields = flds - categorical_fields
        facflds = flds - ignore_fields

        # facets(field) = set(value-1, value-2, ...)
        facets = {
            f: set([str(r[f]) for r in recs if not isinstance(r[f], list)])
            for f in facflds
        }
        return dict(
            fields=flds,
            facet_fields=facflds,
            facets=facets,
        )

    def __str__(self):
        return f"{self.service}-{self.primary_endpoint} [{self.endpoints}]"


# END: class SourceAdapter


# Not available on SLAC (usdf) as of 9/9/2024.
class NightReportAdapter(SourceAdapter):
    outfields = {
        "confluence_url",
        "date_added",
        "date_invalidated",
        "date_sent",
        "day_obs",
        "id",
        "is_valid",
        "observers_crew",
        "parent_id",
        "site_id",
        "summary",
        "telescope",
        "telescope_status",
        "user_agent",
        "user_id",
    }
    service = "nightreport"
    endpoints = ["reports"]
    primary_endpoint = "reports"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
    ):
        super().__init__(max_dayobs=max_dayobs, min_dayobs=min_dayobs)
        self.server = server_url if server_url else SourceAdapter.server
        self.limit = SourceAdapter.limit if limit is None else limit

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        if self.min_date:
            self.status[self.primary_endpoint] = self.get_records()

    # Nightreport
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        row_str_func=None,
        zero_message=False,
    ):
        """Break on TELESCOPE, DATE. Within that only show time."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                dt = datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(dt)

        def obs_date(rec):
            dt = datetime.fromisoformat(rec[datetime_field])
            return dt.replace(microsecond=0)

        def telescope(rec):
            return rec["telescope"]

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return

        table = list()
        # Sort by TELESCOPE, then by OBS_DATE.
        recs = sorted(recs, key=obs_date)
        recs = sorted(recs, key=telescope)
        for tele, g0 in itertools.groupby(recs, key=telescope):
            table.append(f"### Telescope: {tele}")
            for rec in g0:
                attrstr = f'{str(obs_date(rec))} {rec.get("user_id")}'
                table.append(f"{self.row_str_func(attrstr, rec)}")
                crew_list = rec.get("observers_crew", [])
                crew_str = ", ".join(crew_list)
                status = rec.get("telescope_status", "NA")
                table.append(f"Telescope Status: *{status}*")
                table.append(f"Observer Crew: *{crew_str}*")
        return table

    def row_str_func(self, datetime_str, rec):
        msg = rec["summary"].strip()
        return f"`{datetime_str}`\n```\n{msg}\n```"

    def get_records(
        self,
        site_ids=None,
        summary=None,
        is_human="either",
        is_valid="true",
    ):
        qparams = dict(is_human=is_human, is_valid=is_valid)
        if site_ids:
            qparams["site_ids"] = site_ids
        if summary:
            qparams["summary"] = summary
        if self.min_dayobs:
            qparams["min_day_obs"] = ut.dayobs_int(self.min_dayobs)
        if self.max_dayobs:
            qparams["max_day_obs"] = ut.dayobs_int(self.max_dayobs)
        if self.limit:
            qparams["limit"] = self.limit

        qstr = urlencode(qparams)
        url = f"{self.server}/{self.service}/reports?{qstr}"
        error = None
        content = None
        try:
            response = requests.get(url, timeout=self.timeout)
            content = response.text
            validate_response(response, url)
            recs = response.json()
            recs.sort(key=lambda r: r["day_obs"])
        except Exception as err:
            recs = []
            error = f"{content=} Exception={err}"

        self.keep_fields(recs, self.outfields)
        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
        )
        return status

    def nightly_tickets(self):
        tickets = defaultdict(set)  # tickets[day_obs] = {ticket_url, ...}
        for r in self.records:
            ticket_url = r["confluence_url"]
            if ticket_url:
                tickets[r["day_obs"]].add(ticket_url)
        return {dayobs: list(urls) for dayobs, urls in tickets.items()}


class NarrativelogAdapter(SourceAdapter):
    outfields = {
        # 'category',
        # 'components',
        # 'cscs',
        "date_added",
        # 'date_begin',
        # 'date_end',
        # 'date_invalidated',
        # 'id',
        # 'is_human',
        # 'is_valid',
        # 'level',
        "message_text",
        # 'parent_id',
        # 'primary_hardware_components',
        # 'primary_software_components',
        # 'site_id',
        # 'subsystems',
        # 'systems',
        # 'tags',
        "time_lost",
        "time_lost_type",
        # 'urls',
        # 'user_agent',
        "user_id",
    }
    service = "narrativelog"
    endpoints = [
        "messages",
    ]
    primary_endpoint = "messages"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
    ):
        super().__init__(max_dayobs=max_dayobs, min_dayobs=min_dayobs)
        self.server = server_url if server_url else SourceAdapter.server
        self.limit = SourceAdapter.limit if limit is None else limit

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        if self.min_date:
            self.status[self.primary_endpoint] = self.get_records()

    # Narrativelog
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        row_str_func=None,
        zero_message=False,
    ):
        """Break on DATE. Within that show time, author."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                dt = datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(dt)

        def obs_date(rec):
            dt = datetime.fromisoformat(rec[datetime_field])
            return dt.replace(microsecond=0)

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return

        table = list()

        recs = sorted(recs, key=obs_date)
        recs = sorted(recs, key=obs_night)
        for tele, g0 in itertools.groupby(recs, key=obs_night):
            for rec in g0:
                attrstr = f'{str(obs_date(rec))} {rec.get("user_id")}'
                table.append(f"{self.row_str_func(attrstr, rec)}")
        return table

    def get_records(
        self,
        site_ids=None,
        message_text=None,
        is_human="either",
        is_valid="true",
        offset=None,
    ):
        qparams = dict(
            is_human=is_human,
            is_valid=is_valid,
            order_by="-date_begin",
        )
        if site_ids:
            qparams["site_ids"] = site_ids
        if message_text:
            qparams["message_text"] = message_text
        if self.min_date:
            qparams["min_date_added"] = datetime.combine(
                self.min_date, time()
            ).isoformat()
        if self.max_date:
            qparams["max_date_added"] = datetime.combine(
                self.max_date, time()
            ).isoformat()
        if self.limit:
            qparams["limit"] = self.limit

        qstr = urlencode(qparams)
        url = f"{self.server}/{self.service}/messages?{qstr}"
        error = None
        try:
            r = requests.get(url, timeout=self.timeout)
            validate_response(r, url)
            recs = r.json()
            recs.sort(key=lambda r: r["date_begin"])
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

    def get_timelost(self, recs, rollup="day"):
        def iso_date_begin(rec):
            return datetime.fromisoformat(rec["date_begin"]).date().isoformat()

        day_tl = dict()  # day_tl[day] = totalDayTimeLost
        for day, dayrecs in itertools.groupby(recs, key=iso_date_begin):
            day_tl[day] = sum([r["time_lost"] for r in dayrecs])
        return day_tl


# END: class NarrativelogAdapter


class ExposurelogAdapter(SourceAdapter):
    ignore_fields = ["id"]
    outfields = {
        "date_added",
        # 'date_invalidated',
        "day_obs",
        # 'exposure_flag',
        # 'id',
        "instrument",
        # 'is_human',
        # 'is_valid',
        # 'level',
        "message_text",
        "obs_id",
        # 'parent_id',
        # 'seq_num',
        # 'site_id',
        # 'tags',
        # 'urls',
        # 'user_agent',
        # 'user_id',
    }
    service = "exposurelog"
    endpoints = [
        "instruments",
        "exposures",
        "messages",
    ]
    primary_endpoint = "messages"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
    ):
        super().__init__(max_dayobs=max_dayobs, min_dayobs=min_dayobs)
        self.server = server_url if server_url else SourceAdapter.server
        self.limit = SourceAdapter.limit if limit is None else limit

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()
        self.instruments = dict()  # dict[instrument] = registry

        self.exposures = dict()  # dd[instrument] = [rec, ...]

        # Load the data (records) we need from relevant endpoints
        # in dependency order.
        self.status["instruments"] = self.get_instruments()
        for instrument in self.instruments.keys():
            endpoint = f"exposures.{instrument}"
            self.status[endpoint] = self.get_exposures(instrument)
        if self.min_date:
            self.status[self.primary_endpoint] = self.get_records()

    @property
    def row_header(self):
        return (
            "| Time | OBS ID | Instrument | Message |\n"
            "|------|--------|------------|---------|"
        )

    def row_str_func(self, datetime_str, rec):
        msg = rec["message_text"].strip()
        return (
            f"> {datetime_str} "
            f"| {rec['obs_id']} "
            f"| {rec['instrument']} "
            # f"| <pre>{msg}</pre>"
            f"\n```\n{msg}\n```"
        )

    # Exposurelog
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        row_str_func=None,
        zero_message=False,
    ):
        """Break on INSTRUMENT, DATE. Within that only show time."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                dt = datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(dt)

        def obs_date(rec):
            dt = datetime.fromisoformat(rec[datetime_field])
            return dt.replace(microsecond=0)

        def instrument(rec):
            return rec["instrument"]

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return

        table = list()
        # Sort by INSTRUMENT, then by OBS_DATE.
        recs = sorted(recs, key=obs_date)
        recs = sorted(recs, key=instrument)
        for instrum, g0 in itertools.groupby(recs, key=instrument):
            table.append(f"### Instrument: {instrum}")
            for rec in g0:
                attrstr = f'{str(obs_date(rec))} {rec.get("user_id")}'
                table.append(f"{self.row_str_func(attrstr, rec)}")
        return table

    def check_endpoints(self, timeout=None, verbose=True):
        to = timeout or self.timeout
        if verbose:
            msg = "Try connect to each endpoint of "
            msg += f"{self.server}/{self.service} "
            print(msg)
        url_http_status_code = dict()

        for ep in self.endpoints:
            qstr = "?instrument=na" if ep == "exposures" else ""
            url = f"{self.server}/{self.service}/{ep}{qstr}"
            try:
                r = requests.get(url, timeout=to)
                validate_response(r, url)
            except Exception:
                url_http_status_code[url] = "GET error"
            else:
                url_http_status_code[url] = r.status_code
        allgood_p = all([v == 200 for v in url_http_status_code.values()])
        return url_http_status_code, allgood_p

    def get_instruments(self):
        url = f"{self.server}/{self.service}/instruments"
        recs = dict(dummy=[])
        error = None
        try:
            r = requests.get(url, timeout=self.timeout)
            validate_response(r, url)
            recs = r.json()
        except Exception as err:
            error = str(err)
        else:
            self.instruments = {
                instrum: int(reg.replace("butler_instruments_", ""))
                for reg, inst_list in recs.items()
                for instrum in inst_list
            }
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
        )
        return status

    # RETURNS status: dict[endpoint_url, number_of_records, error]
    # SIDE-EFFECT: puts records in self.exposures
    def get_exposures(self, instrument, verbose=False):
        registry = self.instruments[instrument]
        qparams = dict(instrument=instrument, registry=registry)
        if self.min_dayobs:
            qparams["min_day_obs"] = ut.dayobs_int(self.min_dayobs)
        if self.max_dayobs:
            qparams["max_day_obs"] = ut.dayobs_int(self.max_dayobs)
        url = f"{self.server}/{self.service}/exposures?{urlencode(qparams)}"
        recs = []
        error = None
        try:
            if verbose:
                print(f"DBG get_exposures {url=}")
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

    def get_records(
        self,
        site_ids=None,
        obs_ids=None,
        instruments=None,
        message_text=None,
        is_human="either",
        is_valid="true",
        exposure_flags=None,
    ):
        qparams = dict(
            is_human=is_human,
            is_valid=is_valid,
            order_by="-date_added",
        )
        if site_ids:
            qparams["site_ids"] = site_ids
        if obs_ids:
            qparams["obs_ids"] = obs_ids
        if instruments:
            qparams["instruments"] = instruments
        if self.min_dayobs:
            qparams["min_day_obs"] = ut.dayobs_int(self.min_dayobs)
        if self.max_dayobs:
            qparams["max_day_obs"] = ut.dayobs_int(self.max_dayobs)
        if exposure_flags:
            qparams["exposure_flags"] = exposure_flags
        if self.limit:
            qparams["limit"] = self.limit

        qstr = urlencode(qparams)
        url = f"{self.server}/{self.service}/messages?{qstr}"
        recs = []
        error = None
        try:
            response = requests.get(url, timeout=self.timeout)
            validate_response(response, url)
            recs = response.json()
        except Exception as err:
            error = str(err)

        if recs:
            recs.sort(key=lambda r: r["day_obs"])

        self.keep_fields(recs, self.outfields)
        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
        )
        return status

    def get_observation_gaps(self):
        def day_func(r):
            return r["day_obs"]

        # inst_day_rollup[instrument] => dict[day] => exposureGapInMinutes
        inst_day_rollup = defaultdict(dict)  # Instrument/Day rollup
        for instrum in self.instruments.keys():
            recs = self.exposures[instrum]
            instrum_gaps = dict()
            for day, dayrecs in itertools.groupby(recs, key=day_func):
                gaps = list()
                begin = end = None
                for rec in dayrecs:
                    begin = datetime.fromisoformat(rec["timespan_begin"])
                    if end:
                        # span in minutes
                        diff = (begin - end).total_seconds() / 60.0
                        tuple = (
                            end.time().isoformat(),
                            begin.time().isoformat(),
                            diff,
                        )
                        gaps.append(tuple)
                    end = datetime.fromisoformat(rec["timespan_end"])
                instrum_gaps[day] = gaps

                # Rollup gap times by day
                for day, tuples in instrum_gaps.items():
                    inst_day_rollup[instrum][day] = sum([t[2] for t in tuples])

        return inst_day_rollup


# END: class ExposurelogAdapter

adapters = [
    ExposurelogAdapter,
    NarrativelogAdapter,
    NightReportAdapter,
]
