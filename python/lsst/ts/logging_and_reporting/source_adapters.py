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

# Its common to get "dirty data" from sources.
# Examples:
#   - A source return values of both None (python type) and "None" (string).
#   - Values of np.nan (numpy.float64) where py None is more approprite.
#   - return LIST that always has one element (careful about "always")
#   - string to represent True and False (careful of tri-state)
#   - No type for a field when a strict one (pd.astype) would help.
# TODO: clean up dirty data
# TODO: Add dtypes to DFs returned by allsrc.get_sources_time_logs()
# TODO: Every adapter should raise exception when I cannot get data.
#       (getting empty set should not be exception)

import copy
import datetime as dt
import itertools
import re
import traceback
import warnings
from abc import ABC
from collections import defaultdict
from urllib.parse import urlencode

import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.parse_message as pam
import lsst.ts.logging_and_reporting.reports as rep
import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd
import requests

MAX_CONNECT_TIMEOUT = 7.05  # seconds
MAX_READ_TIMEOUT = 180  # seconds

maximum_record_limit = 9000


def all_endpoints(server):
    endpoints = itertools.chain.from_iterable(
        [sa(server_url=server).used_endpoints() for sa in adapters]
    )
    return list(endpoints)


def OBSOLETE_invalid_response(response, endpoint_url, timeout=None, verbose=False):
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

    abbrev = "UNK"  # UNKnown
    default_record_limit = 10  # Adapter specific default

    # TODO document class including all class variables.
    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=max_dayobs - 1 day
        max_dayobs=None,  # EXCLUSIVE: default=TODAY other=YYYY-MM-DD
        offset=0,
        limit=None,  # max records to read in one API call
        connect_timeout=5.05,  # seconds
        read_timeout=20,  # seconds
        verbose=True,
        warning=True,
        auth_token=None,
    ):
        """Load the relevant data for the Source.

        Intended to load from all used Source endpoints over the range
        of dayobs specified in INIT. The day(s) records for each
        endpoint is stored for later use.  Do not make the dayobs
        range large or you will use lots of memory. Tens of days is probably
        ok.
        """

        self.server = server_url or ut.Server.get_url()
        self.verbose = verbose
        self.warning = warning
        self.offset = offset
        if limit is None:
            limit = self.__class__.default_record_limit
        self.limit = min(limit, maximum_record_limit)
        cto = float(connect_timeout)
        self.c_timeout = min(MAX_CONNECT_TIMEOUT, cto)  # seconds
        self.r_timeout = min(MAX_READ_TIMEOUT, float(read_timeout))  # seconds
        self.timeout = (self.c_timeout, self.r_timeout)

        self.token = auth_token

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

        if self.min_date > self.max_date:
            self.min_date, self.max_date = self.max_date, self.min_date

        self.min_dayobs = ut.datetime_to_dayobs(self.min_date)

    def __str__(self):
        return (
            f"{self.server}: {self.min_dayobs}, {self.max_dayobs}; "
            f"{self.service} endpoints={self.endpoints}"
        )

    def __repr__(self):
        cname = self.__class__.__name__
        return (
            f"{cname}(server_url={self.server!r}, "
            f"min_dayobs={self.min_dayobs!r}, "
            f"max_dayobs={self.max_dayobs!r})"
        )

    def records_to_dataframe(self, records, wrap_columns=True):
        df = pd.DataFrame(records)
        if wrap_columns:
            return ut.wrap_dataframe_columns(df)
        else:
            return df

    def protected_post(self, url, jsondata, timeout=None):
        """Do a POST against an API url.
        Do NOT stop processing when we have a problem with a URL. There
        have been cases where the problem has been with
        connectivity or API functioning. We want to process as many of our
        sources as possible even if one or more fail.  But we want to
        KNOW that we had a problem so we can report it to someone.

        RETURN: If the POST works well: ok=True, result=json
        RETURN: If the POST is bad: ok=False, result=error_msg_string
        """
        ok = True
        code = 200
        timeout = timeout or self.timeout
        if self.verbose:
            print(f"DEBUG protected_post({url=},{timeout=})")
        try:
            response = requests.post(
                url,
                json=jsondata,
                timeout=timeout,
                headers=ut.get_auth_header(self.token),
            )
            if self.verbose:
                print(
                    f"DEBUG protected_post({url=},{ut.get_auth_header(self.token)=},{timeout=}) => "
                    f"{response.status_code=} {response.reason}"
                )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            # Invalid URL?, etc.
            ok = False
            code = err.response.status_code
            reason = err.response.reason
            result = f"[{self.abbrev}] Error getting data from API at {url}. "
            result += f"{jsondata=} {timeout=} {reason=} "
            result += f"; {str(err)}."
            result += f"; {err.response.json()['message']=}."
        except requests.exceptions.ConnectionError as err:
            # No VPN? Broken API?
            ok = False
            code = None
            result = f"Error connecting to {url}. "
            result += f"{jsondata=} {timeout=} "
            result += f"; {str(err)}."
        else:  # No exception. Could something else be wrong?
            result = response.json()

        if self.verbose and not ok:
            print(f"DEBUG protected_post: FAIL: {result=}")

        # when ok=True, result is records (else error message)
        return ok, result, code

    def protected_get(self, url, timeout=None):
        """Do a GET against an API url.
        Do NOT stop processing when we have a problem with a URL. There
        have been cases where the problem has been with
        connectivity or API functioning. We want to process as many of our
        sources as possible even if one or more fail.  But we want to
        KNOW that we had a problem so we can report it to someone.

        RETURN: If the GET works well: ok=True, result=json
        RETURN: If the GET is bad: ok=False, result=error_msg_string
        """
        ok = True
        code = 200
        timeout = timeout or self.timeout
        if self.verbose:
            print(f"DEBUG protected_get({url=},{timeout=})")
        try:
            response = requests.get(
                url, timeout=timeout, headers=ut.get_auth_header(self.token)
            )
            if self.verbose:
                print(
                    f"DEBUG protected_get({url=},{ut.get_auth_header(self.token)=},{timeout=}) => "
                    f"{response.status_code=} {response.reason}"
                )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            # Invalid URL?, etc.
            traceback.print_exc()
            ok = False
            code = err.response.status_code
            reason = err.response.reason
            result = f"[{self.abbrev}] Error getting data from API at {url}. "
            result += f"{timeout=} {reason=} "
            result += str(err)
        except requests.exceptions.ConnectionError as err:
            # No VPN? Broken API?
            traceback.print_exc()
            ok = False
            code = None
            result = f"Error connecting to {url} (with timeout={timeout}). "
            result += str(err)
        except Exception as err:
            traceback.print_exc()
            ok = False
            code = None
            result = f"Error getting data from API at {url}. {err}"
        else:  # No exception. Could something else be wrong?
            if self.verbose:
                print(
                    f"DEBUG protected_get: {response.status_code=} "
                    f"{response.reason=}"
                )
            result = response.json()
            if self.verbose:
                print(f"DEBUG protected_get: {len(result)=}")

        if self.verbose and not ok:
            print(f"DEBUG protected_get: FAIL: {result=}")
        return ok, result, code

    def hack_reconnect_after_idle(self):
        """Do a dummy query to a service to force a DB reconnect.

        When a connection has been idle for some time, it disconnects
        such that the following API call returns zero records. This
        HACK gets around this problem.

        TODO After DM-43835 is fixed, remove this hack.
        """
        endpoint = f"{self.server}/{self.service}/{self.primary_endpoint}"
        qparams = dict(limit=2)  # API requires > 1 !
        url = f"{endpoint}?{urlencode(qparams)}"
        try:
            requests.get(
                url, timeout=self.timeout, headers=ut.get_auth_header(self.token)
            )
        except Exception:
            pass  # this is a hack to force reconnect. Response irrelevent.

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

    def check_endpoints(self, verbose=True):
        self.hack_reconnect_after_idle()
        if verbose:
            msg = f"Try to connect ({self.timeout=}) to each endpoint of "
            msg += f"{self.server}/{self.service} "
            print(msg)

        url_http_status_code = dict()
        for ep in self.endpoints:
            url = f"{self.server}/{self.service}/{ep}"
            ok, result, status_code = self.protected_get(url)
            url_http_status_code[url] = status_code if ok else "GET error"

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


# END: class SourceAdapter


class NightReportAdapter(SourceAdapter):
    abbrev = "NIG"
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
        "weather",
        "maintel_summary",
        "auxtel_summary",
        "user_agent",
        "user_id",
    }
    default_record_limit = 100  # Adapter specific default
    service = "nightreport"
    endpoints = ["reports"]
    primary_endpoint = "reports"  # for time-log
    log_dt_field = "date_added"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
        verbose=False,
        warning=False,
        auth_token=None,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
            warning=warning,
            auth_token=auth_token,
        )

        # status[endpoint] = dict(endpoint_url, number_of_records, error)
        self.status = dict()

        # Load the data (records) we need from relevant endpoints
        if self.min_date:
            self.hack_reconnect_after_idle()
            self.status[self.primary_endpoint] = self.get_records()

    @property
    def sources(self):
        return {
            "Nightreport API": (
                f"{self.server}/{self.service}/{self.primary_endpoint}"
            )
        }

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
                # Replace 3 or more newlines with just two.
                msg = re.sub(r"\n{3,}", "\n\n", rec["summary"].strip())

                table.append(f"\n{msg}\n")
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
            print(f"Debug get_records {endpoint=}")

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

        url = None
        recs = []
        while len(recs) <= maximum_record_limit:
            if self.verbose:
                print(f"Debug get_records qstr: {urlencode(qparams)}")
            url = f"{endpoint}?{urlencode(qparams)}"
            ok, result, code = self.protected_get(url)
            if not ok:  # failure
                status = dict(
                    endpoint_url=url,
                    number_of_records=None,
                    error=result,
                )
                break
            page = result
            recs += page
            status = dict(endpoint_url=url, number_of_records=len(recs), error=None)
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)
        # END: while

        self.records = recs
        if recs and self.verbose:
            print(f"Debug get_records-2 {len(page)=} {len(recs)=}")
            print(f"Debug get_records-2 {len(self.records)=} {status=}")

        return status

    def nightly_tickets(self):
        tickets = defaultdict(set)  # tickets[day_obs] = {ticket_url, ...}
        for r in self.records:
            ticket_url = r["confluence_url"]
            if ticket_url:
                tickets[r["day_obs"]].add(ticket_url)
        return {dayobs: list(urls) for dayobs, urls in tickets.items()}


class NarrativelogAdapter(SourceAdapter):
    abbrev = "NAR"
    outfields = {
        "category",
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
        "site_id",
        "tags",
        "time_lost",
        "time_lost_type",
        "urls",
        "user_agent",
        "user_id",
        # ## The following are deprecated. Removed in v1.0.0.
        # ## Use 'components_path' (components_json) instead
        # "subsystems",
        # "systems",
        # "cscs",
        # "components",
        # "primary_hardware_components",
        # "primary_software_components",
        "components_json",
    }
    default_record_limit = 1000  # Adapter specific default
    service = "narrativelog"
    endpoints = [
        "messages",
    ]
    primary_endpoint = "messages"
    log_dt_field = "date_added"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
        verbose=False,
        warning=False,
        auth_token=None,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
            warning=warning,
            auth_token=auth_token,
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
    def sources(self):
        return {
            "Narrative Log API": f"{self.server}/{self.service}/{self.primary_endpoint}"
        }

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

        # Sort by OBS_NIGHT within that by OBS_DATE (datetime)
        recs = sorted(recs, key=obs_date)
        recs = sorted(recs, key=obs_night)
        # time_lost_type=weather is RARE
        for tele, g0 in itertools.groupby(recs, key=obs_night):
            for rec in g0:
                rec_dt = str(dt.datetime.fromisoformat(rec[datetime_field]))[:16]

                attrstr = ""
                attrstr += f"**{rec_dt}**"
                if rec.get("components"):
                    complist = ", ".join(rec.get("components", []))
                    attrstr += f"   **{complist}**"

                if rec.get("time_lost", 0) > 0:
                    attrstr += f" Time Lost: {rec.get('time_lost')};"
                    attrstr += f" Time Lost Type: {rec.get('time_lost_type')};"
                new = rec.get("error_message")
                mdstr = ""
                if new:
                    msg = new
                else:

                    # Replace 3 or more newlines with just two.
                    msg = rep.htmlcode(
                        re.sub(r"\n{3,}", "\n\n", rec["message_text"].strip())
                    )
                    mdstr += f"- {attrstr}"

                mdstr += "\n\n" + msg + "\n"
                table.append(mdstr)

                if rec.get("urls"):
                    for url in rec.get("urls"):
                        table.append(f"- Link: {rep.mdpathlink(url)}")
        return table

    # figure out instrument name from telescope name
    def add_instrument(self, records):
        """Add 'instrument' field to records (SIDE-EFFECT)
        Narrativelog gives Telescope (field="components") but not Instrument,
        but we need to report by Instrument since that is what is used for
        exposures.
        Therefore, we must map Telescope to the Instrument that is assumed to
        be on the Telescope.
        We always assume Telescope=AuxTel means Instrument=LATISS.
        For Telescope=MainTel (aka Simonyi) the Instrument assumed is different
        depending on the date when the log was added.
        Prior to 2025-01-19 we assume Instrument=LSSTComCam
        from then on we assume Instrument=LSSTCam.
        For any Telescope value other than AuxTel or MainTel we ignore the data
        (but warn about what Telescope we are ignoring).

        Parameters
        ----------
        records : `list` [`dict`]
            List of records to process.

        Returns
        -------
        updated_records : `list` [`dict`]
            List of records with 'instrument' field added.
        """

        LSST_DAY = 20250120
        for rec in records:
            day_added = int(rec["date_added"][:10].replace("-", ""))
            if rec["components_json"] is None:
                instrument = None
            elif rec["components_json"].get("name") == "AuxTel":
                instrument = "LATISS"
            elif rec["components_json"].get("name") == "MainTel":
                instrument = "lsst"
            elif rec["components_json"].get("name") == "Simonyi":
                instrument = "lsst"
            else:
                instrument = None

            if self.warning and instrument is None:
                components = rec["components"]
                dateadded = rec["date_added"]
                msg = (
                    'Unknown Telescope found in "components" field '
                    "of a record in the Narrative Log "
                    f"added on {dateadded}. "
                    "Expected one of {AuxTel, MainTel, Simonyi} "
                    f"got {components=}. "
                )
                warnings.warn(
                    msg, category=ex.UnknownTelescopeWarning, stacklevel=2
                )

            if instrument == "lsst":
                if day_added >= LSST_DAY:
                    rec["instrument"] = "LSSTCam"
                else:
                    rec["instrument"] = "LSSTComCam"
            else:
                rec["instrument"] = instrument

        return records

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
            order_by="-date_begin",
            offset=0,
            limit=self.limit,
        )
        if site_ids:
            qparams["site_ids"] = site_ids
        if message_text:
            qparams["message_text"] = message_text
        if self.min_date:
            qparams["min_date_begin"] = dt.datetime.combine(
                self.min_date, dt.time(12, 0)
            ).isoformat()
        if self.max_date:
            qparams["max_date_begin"] = dt.datetime.combine(
                self.max_date, dt.time(11, 59, 59)
            ).isoformat()

        error = None
        recs = []
        while len(recs) <= maximum_record_limit:
            url = f"{endpoint}?{urlencode(qparams)}"
            if self.verbose:
                print(f"Debug get_records qstr: {urlencode(qparams)}")
            ok, result, code = self.protected_get(url)
            if not ok:  # failure
                status = dict(
                    endpoint_url=url,
                    number_of_records=None,
                    error=result,
                )
                break
            page = result
            recs += page
            status = dict(
                endpoint_url=url,
                number_of_records=len(recs),
                error=error,
            )

            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)
        # END: while

        self.records = self.add_instrument(recs)
        pam.markup_errors(self.records)
        return status

    def verify_records(self):
        telescope_fault_loss = [
            (r["date_added"], r["time_lost"], r["time_lost_type"], r["components"])
            for r in self.records
            if r["time_lost"] > 0
        ]
        return telescope_fault_loss

    # END: class NarrativelogAdapter


class ExposurelogAdapter(SourceAdapter):
    abbrev = "EXP"
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
    log_dt_field = "date_added"

    def __init__(
        self,
        *,
        server_url=None,
        min_dayobs=None,  # INCLUSIVE: default=Yesterday
        max_dayobs=None,  # EXCLUSIVE: default=Today other=YYYY-MM-DD
        limit=None,
        verbose=False,
        warning=False,
        auth_token=None,
    ):
        super().__init__(
            server_url=server_url,
            max_dayobs=max_dayobs,
            min_dayobs=min_dayobs,
            limit=limit,
            verbose=verbose,
            warning=warning,
            auth_token=auth_token,
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

    @property
    def sources(self):
        return {
            "Exposure Log API": f"{self.server}/{self.service}/{self.primary_endpoint}"
        }

    # SIDE-EFFECT: Modifies self.exp_src.exposures in place.429
    def add_exposure_flag_to_exposures(self):
        count = 0
        for instrument in self.exposures.keys():
            new_recs = list()
            for rec in self.exposures[instrument]:
                new_rec = copy.copy(rec)
                mrec = self.messages_lut.get(rec["obs_id"])
                if mrec is None:
                    new_rec["exposure_flag"] = "no flag"
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
                    eflag = rec.get("exposure_flag")
                    if eflag == "junk":
                        flag = rep.htmlbad
                    elif eflag == "questionable":
                        flag = rep.htmlquestion
                    else:  # "none", the literal string in API!
                        # value changed to "good" in adapter after read
                        flag = rep.htmlgood
                    msg = rec["message_text"].strip()
                    plinks = [rep.mdpathlink(url) for url in rec.get("urls")]
                    links = ", ".join(plinks)
                    linkstr = "" if links == "" else f"\n    - Links: {links}"

                    # (BLACK workaround)
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
            "exposure_flag",  # joined from exposures.messages
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

    def check_endpoints(self, verbose=True):
        self.hack_reconnect_after_idle()
        if verbose:
            msg = "Try to connect ({self.timeout=}) to each endpoint of "
            msg += f"{self.server}/{self.service} "
            print(msg)

        url_http_status_code = dict()
        for ep in self.endpoints:
            qstr = "?instrument=na" if ep == "exposures" else ""
            url = f"{self.server}/{self.service}/{ep}{qstr}"
            ok, result, status_code = self.protected_get(url)
            url_http_status_code[url] = status_code if ok else "GET error"

        return url_http_status_code, all(
            [v == 200 for v in url_http_status_code.values()]
        )

    def get_instruments(self):
        url = f"{self.server}/{self.service}/instruments"
        recs = dict(dummy=[])
        ok, result, code = self.protected_get(url)
        if not ok:
            status = dict(
                endpoint_url=url,
                number_of_records=None,
                error=result,
            )
            return status

        recs = result
        self.instruments = {
            instrum: int(reg.replace("butler_instruments_", ""))
            for reg, inst_list in recs.items()
            for instrum in inst_list
        }
        status = dict(
            endpoint_url=url,
            number_of_records=len(recs),
            error=None,
        )
        return status

    # RETURNS status: dict[endpoint_url, number_of_records, error]
    # SIDE-EFFECT: puts records in self.exposures
    # /exposurelog/exposures
    # ?registry=2&instrument=LATISS&order_by=-timespan_end&offset=0&limit=50
    def get_exposures(self, instrument):
        endpoint = f"{self.server}/{self.service}/exposures"
        if self.verbose:
            print(f"Debug get_exposures {endpoint=}")

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
        while len(recs) <= maximum_record_limit:
            if self.verbose:
                print(f"Debug get_exposures qstr: {urlencode(qparams)}")
            url = f"{endpoint}?{urlencode(qparams)}"
            ok, result, code = self.protected_get(url)
            if not ok:  # failure
                status = dict(
                    endpoint_url=url,
                    number_of_records=None,
                    error=result,
                )
                break
            page = result
            recs += page
            status = dict(
                endpoint_url=url,
                number_of_records=len(recs),
                error=None,
            )
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)
        # END: while

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
            print(f"Debug get_records: {endpoint=}")

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
                print(f"Debug get_records qstr: {urlencode(qparams)}")
            url = f"{endpoint}?{urlencode(qparams)}"
            ok, result, code = self.protected_get(url)
            if not ok:  # failure
                status = dict(
                    endpoint_url=url,
                    number_of_records=None,
                    error=result,
                )
                return status
            page = result
            recs += page
            status = dict(endpoint_url=url, number_of_records=len(recs), error=None)
            if len(page) < self.limit:
                break  # we defintely got all we asked for
            qparams["offset"] += len(page)

        # Change exposure_flag to avoid confusion with python None type
        for rec in recs:
            if rec.get("exposure_flag") == "none":
                # Does not have a flag, but may have had one in the past
                # Or may just have a message.
                rec["exposure_flag"] = "unknown"

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
