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


import datetime as dt
import itertools
from collections import Counter, defaultdict

import lsst.ts.logging_and_reporting.almanac as alm
import lsst.ts.logging_and_reporting.efd as efd
import lsst.ts.logging_and_reporting.source_adapters as sad
from lsst.ts.logging_and_reporting.utils import hhmmss


class AllSources:
    """Container for all SourceAdapter instances used by LogRep."""

    def __init__(
        self,
        *,
        server_url=None,
        max_dayobs=None,  # INCLUSIVE: default=YESTERDAY other=YYYY-MM-DD
        min_dayobs=None,  # INCLUSIVE: default=(max_dayobs - one_day)
        limit=None,
    ):
        # Load data for all needed sources for the selected dayobs range.
        self.nig_src = sad.NightReportAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )
        self.exp_src = sad.ExposurelogAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )
        self.nar_src = sad.NarrativelogAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
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

        instrument_tally = dict()  # d[instrument] = tally_dict
        almanac = self.alm_src
        total_observable_hrs = almanac.night_hours

        targets = await self.efd_src.get_targets()  # a DataFrame
        if verbose:
            print(
                f"AllSources().get_targets() got {len(targets)} targets "
                f"using date range {self.min_date} to {self.max_date}. "
            )

        if targets.empty:
            return None

        num_slews = targets[["slewTime"]].astype(bool).sum(axis=0).squeeze()
        total_slew_seconds = targets[["slewTime"]].sum().squeeze()

        # per Merlin: There is no practical way to get actual detector read
        # time.  He has done some experiments and inferred that it is
        # 2.3 seconds.  He recommends hardcoding the value.
        mean_detector_hrs = 2.3 / (60 * 60.0)

        # Scot says care only about: ComCam, LSSTCam and  Latiss
        for instrument, records in self.exp_src.exposures.items():
            num_exposures = len(records)
            if num_exposures == 0:
                continue

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
                "Total Night (HH:MM:SS)": hhmmss(total_observable_hrs),  # (a)
                "Total Exposure (HH:MM:SS)": hhmmss(exposure_hrs),  # (b)
                "Number of exposures": num_exposures,  # (c)
                "Number of slews": num_slews,  # (d)
                "Total Detector Read (HH:MM:SS)": hhmmss(detector_hrs),  # (e)
                # Next: (f=e/c)
                "Mean Detector Read (HH:MM:SS)": hhmmss(mean_detector_hrs),
                "Total Slew (HH:MM:SS)": hhmmss(slew_hrs),  # (g)
                "Mean Slew (HH:MM:SS)": hhmmss(slew_hrs / num_slews),  # (g/d)
                "Total Idle (HH:MM:SS)": hhmmss(idle_hrs),  # (i=a-b-e-g)
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

        # Until efd.get_targets is run, it will report 0 records.
        # That method is run in: await allsrc.night_tally_observation_gaps()
        res = {
            src.service: src.status[src.primary_endpoint]["number_of_records"]
            for src in sources
        }
        return res

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
        del facets[fname]
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


def uniform_field_counts(records):
    """Count number of records of each value in a Uniform Fields.
    A Uniform Field is one that only has a small number of values.
    RETURN: dict[fieldname] -> dict[value] -> count
    """
    if len(records) == 0:
        return None
    facets, ignored = get_facets(records)
    facets.pop("day_obs", None)
    facets.pop("instrument", None)
    facets.pop("target_name", None)
    facets.pop("group_name", None)
    return {k: dict(Counter([r[k] for r in records])) for k in facets.keys()}
