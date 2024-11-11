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
# #############################################################################


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

import copy
import datetime as dt
import itertools
import traceback
from abc import ABC
from collections import defaultdict
from urllib.parse import urlencode

import lsst.ts.logging_and_reporting.parse_message as pam
import lsst.ts.logging_and_reporting.reports as rep
import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd
import requests

MAX_CONNECT_TIMEOUT = 7.05  # seconds
MAX_READ_TIMEOUT = 180  # seconds
summit = "https://summit-lsp.lsst.codes"
usdf = "https://usdf-rsp-dev.slac.stanford.edu"
tucson = "https://tucson-teststand.lsst.codes"

default_server = usdf

maximum_record_limit = 9000


def all_endpoints(server):
    endpoints = itertools.chain.from_iterable(
        [sa(server_url=server).used_endpoints() for sa in adapters]
    )
    return list(endpoints)


def invalid_response(response, endpoint_url, timeout=None, verbose=False):
    """Return error string if invalid, else return None"""
    if verbose:
        print(f"DEBUG invalid_response {endpoint_url=}")
    if response.ok:
        return None
    else:
        msg = f"{endpoint_url=} {timeout=} "
        msg += f"{response.status_code=} {response.reason}"
        try:
            msg += f" {response.json()}"
        except Exception as err:
            msg += f" {response.text}; {str(err)}"

        if verbose:
            print(f"DEBUG invalid_response ERROR {msg=}")

        # We want to continue when one source gets errors because
        # other sources may not.
        # raise ex.StatusError(msg)
        rep.display_error(msg)
        msg2 = "TRACEBACK (4 levels)\n"
        msg2 += "".join(traceback.format_stack(limit=4))
        rep.display_error(msg2)
        return msg


class SourceAdapter(ABC):
    """Abstract Base Class for all source adapters."""

    default_record_limit = 10  # Adapter specific default

    # TODO document class including all class variables.
    def __init__(
        self,
        *,
        server_url=None,
        max_dayobs=None,  # EXCLUSIVE: default=TODAY other=YYYY-MM-DD
        min_dayobs=None,  # INCLUSIVE: default=max_dayobs - 1 day
        offset=0,
        limit=None,  # max records to read in one API call
        connect_timeout=5.05,  # seconds
        read_timeout=20,  # seconds
        verbose=False,
    ):
        """Load the relevant data for the Source.

        Intended to load from all used Source endpoints over the range
        of dayobs specified in INIT. The day(s) records for each
        endpoint is stored for later use.  Do not make the dayobs
        range large or you will use lots of memory. Tens of days is probably
        ok.
        """
        self.server = server_url or default_server
        self.verbose = verbose
        self.offset = offset
        if limit is None:
            limit = self.__class__.default_record_limit
        self.limit = min(limit, maximum_record_limit)
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
            self.min_date = self.max_date - dt.timedelta(days=1)
        assert self.min_date < self.max_date
        self.min_dayobs = ut.datetime_to_dayobs(self.min_date)

    def hack_reconnect_after_idle(self):
        """Do a dummy query to a serivce to force a DB reconnect.

        When a connection has been idle for some time, it disconnects
        such that the following API call returns zero records. This
        HACK gets around this problem.

        TODO After DM-43835 is fixed, remove this hack.
        """
        endpoint = f"{self.server}/{self.service}/{self.primary_endpoint}"
        qparams = dict(limit=2)  # API requires > 1 !
        url = f"{endpoint}?{urlencode(qparams)}"
        response = requests.get(url, timeout=self.timeout)
        invalid_response(response, url, timeout=self.timeout)
        return response.status_code

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

    # ABC
    @property
    def urls(self):
        return []

    # ABC
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        # row_str_func=None,
        zero_message=False,
    ):
        """Break on DAYOBS.
        Within that, break on DATE, within that only show time."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                rdt = dt.datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(rdt)

        def obs_date(rec):
            rdt = dt.datetime.fromisoformat(rec[datetime_field])
            return rdt.replace(microsecond=0)

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return
        table = list()
        # Group by night.
        recs = sorted(recs, key=obs_night)
        for night, g0 in itertools.groupby(recs, key=obs_night):
            # Group by date
            table.append(f"## NIGHT: {night}: ")
            for date, g1 in itertools.groupby(g0, key=obs_date):
                table.append(f"### DATE: {date.date()}: ")
                for rec in g0:
                    msg = rec["message_text"].strip()
                    rdt = obs_date(rec)
                    dtstr = str(rdt.time())
                    table.append(f"{dtstr}\n```\n{msg}\n```")
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
                to = timeout or self.timeout
                response = requests.get(url, timeout=to)
                if invalid_response(response, url, timeout=to):
                    url_http_status_code[url] = "GET error"
            except Exception:
                url_http_status_code[url] = "GET error"
            else:
                url_http_status_code[url] = response.status_code
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
        return (
            f"{self.server}: "
            f"{self.min_dayobs} to {self.max_dayobs} [{self.limit}] "
            f"{self.service} endpoints={self.endpoints}"
        )


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
    default_record_limit = 100  # Adapter specific default
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
        verbose=False,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
        )

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        if self.min_date:
            self.hack_reconnect_after_idle()
            self.status[self.primary_endpoint] = self.get_records()

    @property
    def urls(self):
        """RETURN flattened list of all URLs."""
        nig_urls = [
            [r.get("confluence_url", [])]
            for r in self.records
            if r.get("confluence_url") != ""
        ]
        return set(itertools.chain.from_iterable(nig_urls))

    # Nightreport
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        zero_message=False,
    ):
        """Break on TELESCOPE, DATE. Within that only show time."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                rdt = dt.datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(rdt)

        def obs_date(rec):
            rdt = dt.datetime.fromisoformat(rec[datetime_field])
            return rdt.replace(microsecond=0)

        def telescope(rec):
            return rec["telescope"]

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return

        table = list()
        # Sort by TELESCOPE, within that by OBS_DATE.
        recs = sorted(recs, key=obs_date)
        recs = sorted(recs, key=telescope)
        for tele, g0 in itertools.groupby(recs, key=telescope):
            table.append(f"### Telescope: {tele}")
            for rec in g0:
                msg = rec["summary"].strip()
                table.append(f"```\n{msg}\n```")
                crew_list = rec.get("observers_crew", [])
                crew_str = ", ".join(crew_list)
                status = rec.get("telescope_status", "Not Available")
                url = rec.get("confluence_url")
                if url and len(url) > 0:
                    table.append(f"Confluence page: {rep.mdfragmentlink(url)}")
                table.append(f"Telescope Status: {status}")
                table.append(f"*Authors: {crew_str}*")
        return table

    # Night Report
    def get_records(
        self,
        site_ids=None,
        summary=None,
        is_human="either",
        is_valid="true",
    ):
        endpoint = f"{self.server}/{self.service}/{self.primary_endpoint}"
        if self.verbose:
            print(f"DBG get_records {endpoint=}")

        qparams = dict(
            is_human=is_human,
            is_valid=is_valid,
            order_by="-day_obs",
            offset=0,
            limit=self.limit,
        )
        if site_ids:
            qparams["site_ids"] = site_ids
        if summary:
            qparams["summary"] = summary
        if self.min_dayobs:
            qparams["min_day_obs"] = ut.dayobs_int(self.min_dayobs)
        if self.max_dayobs:
            qparams["max_day_obs"] = ut.dayobs_int(self.max_dayobs)

        error = None
        response = None
        url = None
        recs = []
        # try:
        while len(recs) <= maximum_record_limit:
            if self.verbose:
                print(f"DBG get_records qstr: {urlencode(qparams)}")
            url = f"{endpoint}?{urlencode(qparams)}"
            response = requests.get(url, timeout=self.timeout)
            if invalid_response(response, url, timeout=self.timeout):
                break
            page = response.json()
            if self.verbose:
                print(f"DBG get_records {len(page)=} {len(recs)=}")
                recs += page
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)

        self.keep_fields(recs, self.outfields)

        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
        )
        if recs and self.verbose:
            print(f"DBG get_records-2 {len(page)=} {len(recs)=}")
            print(f"DBG get_records-2 {len(self.records)=} {status=}")

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
        "category",
        "components",
        "cscs",
        "date_added",
        "date_begin",
        "date_end",
        "date_invalidated",
        "id",
        "is_human",
        "is_valid",
        "level",
        "message_text",
        "parent_id",
        "primary_hardware_components",
        "primary_software_components",
        "site_id",
        "subsystems",
        "systems",
        "tags",
        "time_lost",
        "time_lost_type",
        "urls",
        "user_agent",
        "user_id",
    }
    default_record_limit = 1000  # Adapter specific default
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
        verbose=False,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
        )
        if self.verbose:
            print(
                "NarrativeLogAdapter("
                f"{server_url=}, {max_dayobs=}, {min_dayobs=}, {limit=}"
            )

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        if self.min_date:
            self.hack_reconnect_after_idle()
            self.status[self.primary_endpoint] = self.get_records()

    @property
    def urls(self):
        """RETURN flattened list of all URLs."""
        rurls = [r.get("urls", []) for r in self.records]
        return set(itertools.chain.from_iterable(rurls))

    # Narrativelog
    def day_table(
        self,
        datetime_field,
        dayobs_field=None,
        zero_message=False,
        use_parser=True,
    ):
        """Break on DATE. Within that show time, author."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                rdt = dt.datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(rdt)

        def obs_date(rec):
            rdt = dt.datetime.fromisoformat(rec[datetime_field])
            return rdt.replace(microsecond=0)

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return

        table = list()

        show_fields = [
            "components",
            "primary_software_components",
            "primary_hardware_components",
        ]

        # Sort by OBS_NIGHT
        recs = sorted(recs, key=obs_night)
        # time_lost_type=weather is RARE
        for tele, g0 in itertools.groupby(recs, key=obs_night):
            for rec in g0:
                rec_dt = dt.datetime.fromisoformat(rec[datetime_field])
                attrstr = ""
                attrstr += f"**{rec_dt}**"
                attrstr += f" Time Lost: {rec.get('time_lost')};"
                attrstr += f" Time Lost Type: {rec.get('time_lost_type')};"
                new = rec.get("error_message")
                if new:
                    msg = new
                else:
                    msg = rep.htmlcode(rec["message_text"].strip())
                    mdstr = ""
                    mdstr += f"- {attrstr}"
                for fname in show_fields:
                    mdstr += f"\n    - {fname}: {rec.get(fname)}"

                mdstr += "\n\n" + msg + "\n"
                table.append(mdstr)

                if rec.get("urls"):
                    for url in rec.get("urls"):
                        table.append(f"- Link: {rep.mdpathlink(url)}")
        return table

    def get_records(
        self,
        site_ids=None,
        message_text=None,
        is_human="either",
        is_valid="true",
    ):
        endpoint = f"{self.server}/{self.service}/{self.primary_endpoint}"
        if self.verbose:
            print(f"Using {endpoint=}")

        qparams = dict(
            is_human=is_human,
            is_valid=is_valid,
            order_by="-date_added",
            offset=0,
            limit=self.limit,
        )
        if site_ids:
            qparams["site_ids"] = site_ids
        if message_text:
            qparams["message_text"] = message_text
        if self.min_date:
            qparams["min_date_added"] = dt.datetime.combine(
                self.min_date, dt.time()
            ).isoformat()
        if self.max_date:
            qparams["max_date_added"] = dt.datetime.combine(
                self.max_date, dt.time()
            ).isoformat()

        error = None
        recs = []
        # try:
        while len(recs) <= maximum_record_limit:
            url = f"{endpoint}?{urlencode(qparams)}"
            if self.verbose:
                print(f"Using {url=}")
            response = requests.get(url, timeout=self.timeout)
            if invalid_response(response, url, timeout=self.timeout):
                break
            page = response.json()
            recs += page
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)
            # except Exception as err:
            #     recs = []
            #     error = str(err)

        self.keep_fields(recs, self.outfields)
        pam.markup_errors(recs)
        self.records = recs
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
        )
        return status

    # END: class NarrativelogAdapter


class ExposurelogAdapter(SourceAdapter):
    ignore_fields = ["id"]
    outfields = {
        "date_added",
        "date_invalidated",
        "day_obs",
        "exposure_flag",
        "id",
        "instrument",
        "is_human",
        "is_valid",
        "level",
        "message_text",
        "obs_id",
        "parent_id",
        "seq_num",
        "site_id",
        "tags",
        "urls",
        "user_agent",
        "user_id",
    }
    default_record_limit = 2500  # Adapter specific default
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
        verbose=False,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
        )

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()
        self.instruments = dict()  # dict[instrument] = registry

        self.exposures = dict()  # dd[instrument] = [rec, ...]

        # Load the data (records) we need from relevant endpoints
        # in dependency order.
        self.hack_reconnect_after_idle()
        self.status["instruments"] = self.get_instruments()
        for instrument in self.instruments.keys():
            endpoint = f"exposures.{instrument}"
            self.status[endpoint] = self.get_exposures(instrument)
        if self.min_date:
            self.status[self.primary_endpoint] = self.get_records()
        # Copy exposure_flag from messages to exposures (some to many).
        self.add_exposure_flag_to_exposures()

    # SIDE-EFFECT: Modifies self.exp_src.exposures in place.429
    def add_exposure_flag_to_exposures(self):
        count = 0
        for instrument in self.exposures.keys():
            new_recs = list()
            for rec in self.exposures[instrument]:
                new_rec = copy.copy(rec)
                mrec = self.messages_lut.get(rec["obs_id"])
                if mrec is None:
                    new_rec["exposure_flag"] = "unknown"
                else:
                    count += 1
                    new_rec["exposure_flag"] = mrec["exposure_flag"]
                    if self.verbose:
                        print(f"add_exposure_flag_to_exposures {rec=}")
                new_recs.append(new_rec)
            self.exposures[instrument] = new_recs
        return count

    @property
    def urls(self):
        """RETURN flattened list of all URLs."""
        rurls = [r.get("urls", []) for r in self.records]
        return set(itertools.chain.from_iterable(rurls))

    # Exposurelog
    def day_table(self, datetime_field, dayobs_field=None, zero_message=False):
        """Break on INSTRUMENT, DATE. Within that only show time."""

        def obs_night(rec):
            if "day_obs" in rec:
                return ut.dayobs_str(rec["day_obs"])  # -> # "YYYY-MM-DD"
            else:
                rdt = dt.datetime.fromisoformat(rec[datetime_field])
                return ut.datetime_to_dayobs(rdt)

        def obs_date(rec):
            rdt = dt.datetime.fromisoformat(rec[datetime_field])
            return rdt.replace(microsecond=0)

        def instrument(rec):
            return rec["instrument"]

        def obs_id(rec):
            return rec["obs_id"]

        recs = self.records
        if len(recs) == 0:
            if zero_message:
                print("Nothing to display.")
            return

        table = list()
        # Sort by INSTRUMENT, then by OBS_ID.
        recs = sorted(recs, key=obs_id)
        recs = sorted(recs, key=instrument)
        for instrum, inst_grp in itertools.groupby(recs, key=instrument):
            inst_list = list(inst_grp)
            table.append(f"### Instrument: {instrum} ({len(inst_list)})")
            for obsid, obs_grp in itertools.groupby(inst_list, key=obs_id):
                obs_list = list(obs_grp)
                rec = obs_list[0]

                attrstr = f"{obsid} : {rec[datetime_field]}"
                for rec in obs_list:
                    match rec.get("exposure_flag"):
                        case "junk":
                            flag = rep.htmlbad
                        case "questionable":
                            flag = rep.htmlquestion
                        case _:  # "none", the literal string in API!
                            # value changed to "good" in adapter after read
                            flag = rep.htmlgood
                            msg = rec["message_text"].strip()
                            plinks = [rep.mdpathlink(url) for url in rec.get("urls")]
                            links = ", ".join(plinks)
                            linkstr = "" if links == "" else f"\n    - Links: {links}"

                    # BLACK workaround
                    str = ""
                    str += f"* {attrstr}"
                    str += f"\n    - {flag}`{msg}`" f"{linkstr}"
                    table.append(str)
        return table

    # /exposurelog/exposures?instrument=LSSTComCamSim
    def exposure_detail(
        self,
        instrument,
        science_program=None,
        observation_type=None,
        observation_reason=None,
    ):
        fields = [
            "obs_id",
            "timespan_begin",  # 'time',
            "seq_num",
            "observation_type",
            "observation_reason",
            "science_program",
            "exposure_time",
            # 'physical_filter',
            # 'nimage',
            # 'hasPD',
            # 'metadata',
        ]

        program = science_program and science_program.lower()
        otype = observation_type and observation_type.lower()
        reason = observation_reason and observation_reason.lower()
        recs = [
            r
            for r in self.exposures[instrument]
            if ((program is None) or (r["science_program"].lower() == program))
            and ((otype is None) or (r["observation_type"].lower() == otype))
            and ((reason is None) or (r["observation_reason"].lower() == reason))
        ]
        if self.verbose:
            print(
                f"exposure_detail({instrument}, "
                f"{science_program=},{observation_type=},{observation_reason=}):"
            )
            print(
                f"{program=} {otype=} {reason=} "
                f"pre-filter={len(self.exposures[instrument])} "
                f"post-filter={len(recs)}"
            )
        if len(recs) > 0:
            df = pd.DataFrame(recs)[fields]
            return ut.wrap_dataframe_columns(df)
        else:
            return pd.DataFrame()

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
                response = requests.get(url, timeout=to)
                err = invalid_response(response, url, timeout=to)
                if err:
                    url_http_status_code[url] = err
            except Exception:
                url_http_status_code[url] = "GET error"
            else:
                url_http_status_code[url] = response.status_code
                allgood_p = all([v == 200 for v in url_http_status_code.values()])
        return url_http_status_code, allgood_p

    def get_instruments(self):
        url = f"{self.server}/{self.service}/instruments"
        recs = dict(dummy=[])
        error = None
        # try:
        response = requests.get(url, timeout=self.timeout)
        err = invalid_response(response, url, timeout=self.timeout)
        if err:
            status = dict(
                endpoint_url=url,
                number_of_records=len(recs),
                error=err,
            )
            return status

        recs = response.json()
        # except Exception as err:
        #     error = str(err)
        # else:
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
    # /exposurelog/exposures
    # ?registry=2&instrument=LATISS&order_by=-timespan_end&offset=0&limit=50
    def get_exposures(self, instrument):
        endpoint = f"{self.server}/{self.service}/exposures"
        if self.verbose:
            print(f"DBG get_exposures {endpoint=}")

        registry = self.instruments[instrument]
        qparams = dict(
            registry=registry,
            instrument=instrument,
            order_by="-timespan_end",
            offset=0,
            limit=self.limit,
        )
        if self.min_dayobs:
            qparams["min_day_obs"] = ut.dayobs_int(self.min_dayobs)
        if self.max_dayobs:
            qparams["max_day_obs"] = ut.dayobs_int(self.max_dayobs)
        recs = []
        error = None
        # try:
        while len(recs) <= maximum_record_limit:
            if self.verbose:
                print(f"DBG get_exposures qstr: {urlencode(qparams)}")
            url = f"{endpoint}?{urlencode(qparams)}"
            response = requests.get(url, timeout=self.timeout)
            if invalid_response(response, url, timeout=self.timeout):
                break
            page = response.json()
            recs += page
            if self.verbose:
                print(f"DBG get_exposures {len(page)=} {len(recs)=}")
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)
        # except Exception as err:
        #     error = str(err)

        # #! sprogram  = rec.get("science_program")
        # #! progstr = sprogram if sprogram else ' '

        for r in recs:
            r["exposure_flag"] = None

        self.exposures[instrument] = recs
        self.exposures_lut = dict()
        for rec in recs:
            exp_secs = (
                dt.datetime.fromisoformat(rec["timespan_end"])
                - dt.datetime.fromisoformat(rec["timespan_end"])
            ).total_seconds()
            rec["exposure_time"] = exp_secs
            self.exposures_lut[rec["obs_id"]] = rec

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
        endpoint = f"{self.server}/{self.service}/messages"
        if self.verbose:
            print(f"DBG get_records: {endpoint=}")

        qparams = dict(
            is_human=is_human,
            is_valid=is_valid,
            order_by="-day_obs",
            offset=0,
            limit=self.limit,
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

        qstr = urlencode(qparams)
        url = f"{endpoint}?{qstr}"
        recs = []
        error = None
        # try:
        while len(recs) <= maximum_record_limit:
            if self.verbose:
                print(f"DBG get_records qstr: {urlencode(qparams)}")
            url = f"{endpoint}?{urlencode(qparams)}"
            response = requests.get(url, timeout=self.timeout)
            if invalid_response(response, url, timeout=self.timeout):
                break
            page = response.json()
            recs += page
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)
        # except Exception as err:
        #     error = str(err)

        self.keep_fields(recs, self.outfields)

        # Change exposure_flag to avoid confusion with python None type
        for rec in recs:
            if rec.get("exposure_flag") == "none":
                rec["exposure_flag"] = "good"

        self.records = recs
        # messages[instrument] => [rec, ...]
        self.messages = dict()
        # messages_lut[obs_id] => rec
        self.messages_lut = {r["obs_id"]: r for r in recs}
        for instrum in set([r["instrument"] for r in recs]):
            self.messages[instrum] = [r for r in recs if instrum == r["instrument"]]

        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=error,
        )
        return status


# END: class ExposurelogAdapter


adapters = [
    ExposurelogAdapter,
    NarrativelogAdapter,
    NightReportAdapter,
]
