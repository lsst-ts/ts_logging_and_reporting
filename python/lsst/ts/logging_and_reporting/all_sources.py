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


import copy
import datetime as dt
import itertools
from collections import Counter, defaultdict
from urllib.parse import urlencode

import lsst.ts.logging_and_reporting.almanac as alm
import lsst.ts.logging_and_reporting.efd as efd
import lsst.ts.logging_and_reporting.source_adapters as sad
import lsst.ts.logging_and_reporting.utils as ut
import pandas as pd


class AllSources:
    """Container for all SourceAdapter instances used by LogRep."""

    def __init__(
        self,
        *,
        server_url=None,
        max_dayobs=None,  # INCLUSIVE: default=YESTERDAY other=YYYY-MM-DD
        min_dayobs=None,  # INCLUSIVE: default=(max_dayobs - one_day)
        limit=None,
        verbose=False,
        exclude_instruments=None,
    ):
        self.verbose = verbose
        self.exclude_instruments = exclude_instruments or []
        ut.tic()
        # Load data for all needed sources for the selected dayobs range.
        self.nig_src = sad.NightReportAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            verbose=verbose,
        )
        self.exp_src = sad.ExposurelogAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            verbose=verbose,
        )
        self.nar_src = sad.NarrativelogAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            verbose=verbose,
        )
        self.alm_src = alm.Almanac(
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )
        self.efd_src = efd.EfdAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )
        # This space for rent by ConsDB

        self.server_url = server_url

        # Get the common min/max date/dayobs from just one source.
        # They are the same for all of them.
        self.max_date = self.nig_src.max_date
        self.min_date = self.nig_src.min_date
        self.max_dayobs = self.nig_src.max_dayobs
        self.min_dayobs = self.nig_src.min_dayobs
        if verbose:
            print(f"Loaded data from sources in {ut.toc():0.1f} seconds")

    # END init

    @property
    def dayobs_range(self):
        return (self.min_dayobs, self.max_dayobs)

    # Our goals are something like this (see DM-46102)
    #
    # Ref                                       Hours
    # -------   ----------------------          ------
    # a         Total Night Hours               9.67
    # b         Total Exposure Hours            1.23
    # d         Number of slews                 16
    # c         Number of exposures             33
    # e         Total Detector Read hours	0.234
    # f=e/c	Mean Detector read hours	0.00709
    # g         Total Slew hours                0.984
    # h=g/d	Mean Slew hours                 0.0615
    # i=a-b-e-g Total Idle Time                 7.222
    #
    # day_obs:: YYYMMDD (int or str)
    # Use almanac begin of night values for day_obs.
    # Use almanac end of night values for day_obs + 1.
    async def night_tally_observation_gaps(self, verbose=False):
        total_observable_hrs = self.alm_src.night_hours
        used_instruments = set()
        for instrum, recs in self.exp_src.exposures.items():
            if instrum in self.exclude_instruments:
                continue
            if len(recs) > 0:
                used_instruments.add(instrum)
        if len(used_instruments) == 0:
            return {
                "": {
                    "Total night": ut.hhmmss(total_observable_hrs),
                    "Idle time": ut.hhmmss(total_observable_hrs),
                }
            }
        if verbose:
            print(
                f"DEBUG night_tally_observation_gaps: "
                f"{used_instruments=} {self.exclude_instruments=}"
            )

        instrument_tally = dict()  # d[instrument] = tally_dict

        # lost[day][lost_type] = totalTimeLost
        # lost = self.get_time_lost()

        # slewTime (and probably others) are EXPECTED times, not ACTUAL.
        # To get actual, need to use TMAEvent or something similar.
        # targets = await self.efd_src.get_targets()  # a DataFrame
        # if verbose:
        #     print(
        #         f"AllSources().get_targets() got {len(targets)} targets "
        #         f"using date range {self.min_date} to {self.max_date}. "
        #     )

        # Merlin might add this to consdb or efd .... eventually
        num_slews = 0
        total_slew_seconds = 0
        mean_slew = 0

        # per Merlin: There is no practical way to get actual detector read
        # time.  He has done some experiments and inferred that it is
        # 2.3 seconds.  He recommends hardcoding the value.
        mean_detector_hrs = 2.3 / (60 * 60.0)

        # Scot says care only about: ComCam, LSSTCam and  Latiss
        for instrument in used_instruments:
            records = self.exp_src.exposures[instrument]
            num_exposures = len(records)

            exposure_seconds = 0
            for rec in records:
                begin = dt.datetime.fromisoformat(rec["timespan_begin"])
                end = dt.datetime.fromisoformat(rec["timespan_end"])
                exposure_seconds += (end - begin).total_seconds()
            detector_hrs = len(records) * mean_detector_hrs

            exposure_hrs = exposure_seconds / (60 * 60.0)
            slew_hrs = total_slew_seconds / (60 * 60.0)
            idle_hrs = (
                total_observable_hrs
                - exposure_hrs
                # - detector_read_hrs
                - slew_hrs
            )

            instrument_tally[instrument] = {
                "Total Night": ut.hhmmss(total_observable_hrs),  # (a)
                "Total Exposure": ut.hhmmss(exposure_hrs),  # (b)
                "Slew time(1)": ut.hhmmss(slew_hrs),  # (g)
                "Readout time(2)": ut.hhmmss(detector_hrs),  # (e)
                "Time loss to fault": "NA",
                "Time loss to weather": "NA",
                "Idle time": ut.hhmmss(idle_hrs),  # (i=a-b-e-g)
                "Number of exposures": num_exposures,  # (c)
                "Mean readout time": mean_detector_hrs,  # (f=e/c)
                "Number of slews(1)": num_slews,  # (d)
                "Mean Slew time(1)": ut.hhmmss(mean_slew),  # (g/d)
            }

        # Composition to combine Exposure and Efd (blackboard)
        # ts_xml/.../sal_interfaces/Scheduler/Scheduler_Events.xml
        # https://ts-xml.lsst.io/sal_interfaces/Scheduler.html#slewtime
        # edf.get_targets() => "slewTime"                             # (d,g,h)
        return instrument_tally

    def records_per_source(self):
        sources = [
            self.nig_src,
            self.exp_src,
            self.nar_src,
            self.efd_src,
        ]
        res = {
            src.service: {
                endpoint: (ed["number_of_records"], ed["endpoint_url"])
                for endpoint, ed in src.status.items()
            }
            for src in sources
        }
        return res

    def get_data_status(self):
        """Get status of data loaded from all sources."""
        sources = [
            self.nig_src,
            self.exp_src,
            self.nar_src,
            self.efd_src,
        ]

        dstat = list()  # [dict(endpoint, rec_count, url), ...]
        for src in sources:
            for endpoint, ed in src.status.items():
                dstat.append(
                    dict(
                        Endpoint=f"{src.service}/{endpoint}",
                        Records=ed["number_of_records"],
                        URL=ed["endpoint_url"],
                    )
                )
        return dstat

    def get_time_lost(self, rollup="day"):
        """RETURN dict[dayobs]['fault', 'weather'] => day_time_lost (hours)"""
        # Units of hours determined my inspection and comparison of:
        #   time_lost, date_begin, date_end

        def date_begin(rec):
            rdt = dt.datetime.fromisoformat(rec["date_begin"])
            return rdt.date().isoformat()

        def lost_type(rec):
            return rec["time_lost_type"]

        day_tl = defaultdict(dict)  # day_tl[day][lost_type] = totalTimeLost
        # Sort by DATE, within that by TYPE
        recs = copy.copy(self.nar_src.records)
        recs = sorted(recs, key=lost_type)
        recs = sorted(recs, key=date_begin)
        for day, day_grp in itertools.groupby(recs, key=date_begin):
            day_tl[day]["fault"] = sum(
                [r["time_lost"] for r in day_grp if "fault" == r["time_lost_type"]]
            )
            day_tl[day]["weather"] = sum(
                [r["time_lost"] for r in day_grp if "weather" == r["time_lost_type"]]
            )
        return day_tl

    def get_observation_gaps(self):
        def day_func(r):
            return r["day_obs"]

        # inst_day_rollup[instrument] => dict[day] => exposureGapInMinutes
        inst_day_rollup = defaultdict(dict)  # Instrument/Day rollup
        instrum_gaps = defaultdict(dict)
        for instrum in self.exp_src.instruments.keys():
            recs = self.exp_src.exposures[instrum]
            for day, dayrecs in itertools.groupby(recs, key=day_func):
                gaps = list()
                durations = list()
                begin = prev_end = None
                for rec in dayrecs:
                    begin = dt.datetime.fromisoformat(rec["timespan_begin"])
                    this_end = dt.datetime.fromisoformat(rec["timespan_end"])
                    exp_secs = (begin - this_end).total_seconds()
                    if prev_end is None:  # First exposure
                        durations.append(exp_secs)
                        tuple = (
                            None,  # Start of exposure gap
                            0,  # Gap duration (minutes
                            begin.time().isoformat(),  # Start of exposure
                            exp_secs,
                        )
                    else:  # Subsequent (non-first) exposures
                        gap_secs = (begin - prev_end).total_seconds()
                        durations.append(exp_secs)
                        durations.append(gap_secs)
                        tuple = (
                            prev_end.time().isoformat(),  # Start of exp GAP
                            gap_secs,
                            begin.time().isoformat(),  # Start of exposure
                            exp_secs,
                        )
                    gaps.append(tuple)
                    prev_end = dt.datetime.fromisoformat(rec["timespan_end"])
                instrum_gaps[instrum][day] = gaps
                # Rollup gap times by day
                for day, tuples in instrum_gaps[instrum].items():
                    inst_day_rollup[instrum][day] = sum([t[1] for t in tuples])

        return inst_day_rollup, instrum_gaps

    def get_slews(self):
        """time when MTMount azimuthInPosition and elevationInPosition events
        have their inPosition items set to False and then again when they
        turn True."""

        pass

    @property
    def urls(self):
        return self.nar_src.urls | self.exp_src.urls

    # WARNING: This combines CONTENT and PRESENTATION.
    def flag_count_exposures(self, instrument, field_name):
        def gen_url(field_value):
            oneday = dt.timedelta(days=1)
            qparams = {
                "day_obs": ut.datetime_to_dayobs(self.max_date - oneday),
                "number_of_days": (self.max_date - self.min_date).days,
                "instrument": instrument,
                field_name: field_value,  # e.g. science_program: BLOCK-T215
            }
            url = f"{self.server_url}/times-square/github/"
            url += "lsst-ts/ts_logging_and_reporting/ExposureDetail"
            url += f"?{urlencode(qparams)}"
            return url

        def gen_link(field_value):
            return f"<a href={gen_url(field_value)}>{field_value}</a>"

        def gen_link_row(row):
            field_value = row["Field Value"]
            return f"<a href={gen_url(field_value)}>{field_value}</a>"

        def mapper(field_value):
            return gen_url(field_value)

        records = self.exp_src.exposures[instrument]

        # field_name: observation_type, observation_reason, science_program
        field_values = {r[field_name] for r in records}
        # Values of rec["exposure_flag"]
        eflag_values = ["good", "questionable", "junk", "unknown"]
        table_recs = defaultdict(dict)
        for field in field_values:
            for eflag in eflag_values:
                # Initialize to zeros
                counter = Counter({f: 0 for f in eflag_values})
                counter.update(
                    [r["exposure_flag"] for r in records if r[field_name] == field]
                )
            table_recs[field]["Detail"] = gen_link(field)
            table_recs[field].update(dict(counter))
            # User want this?: counter.update(dict(total=counter.total()))
        if table_recs:
            df = pd.DataFrame.from_records(
                list(table_recs.values()),
                index=list(table_recs.keys()),
            )
            df.sort_index(inplace=True)
        else:
            df = pd.DataFrame()

        return df

    def fields_count_exposure(self, instrument):
        exposure_field_names = [
            "observation_type",
            "observation_reason",
            "science_program",
        ]
        df_dict = dict()
        for fname in exposure_field_names:
            df_dict[fname] = self.flag_count_exposures(instrument, fname)
        return df_dict


# display(all.get_facets(allsrc.exp_src.exposures['LATISS']))
def get_facets(records, fieldnames=None, ignore_fields=None):
    diversity_theshold = 0.5  # no facets for high numOfUniqueVals/total
    if ignore_fields is None:
        ignore_fields = []
    flds = fieldnames if fieldnames else set(records[0].keys())
    if ignore_fields is None:
        ignore_fields = [fname for fname in flds if "date" in fname]
        ignore_fields.append("day_obs")
    facflds = set(flds) - set(ignore_fields)
    # facets(fieldname) = set(value-1, value-2, ...)
    facets = {
        f: set([str(r[f]) for r in records if not isinstance(r[f], list)])
        for f in facflds
    }

    # Remove facets for fields that are mostly unique across records
    too_diverse = set()
    total = len(records)
    for k, v in facets.items():
        if (len(v) / total) > diversity_theshold:
            too_diverse.add(k)
    for fname in too_diverse:
        facets.pop(fname, None)
        ignore_fields.append(fname)
    return facets, ignore_fields


# pd.DataFrame.from_dict(all.facet_counts(allsrc.exp_src.exposures['LATISS']),
#                        orient='index')
def facet_counts(records, fieldnames=None, ignore_fields=None):
    if len(records) == 0:
        return None
    facets, ignored = get_facets(
        records, fieldnames=fieldnames, ignore_fields=ignore_fields
    )
    fc = {k: len(v) for k, v in facets.items()}
    fc["total"] = len(records)
    return fc
