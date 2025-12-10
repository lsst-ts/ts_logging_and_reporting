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
import warnings
from collections import Counter, defaultdict
from urllib.parse import urlencode

import pandas as pd

import lsst.ts.logging_and_reporting.almanac as alm
import lsst.ts.logging_and_reporting.consdb as cdb
import lsst.ts.logging_and_reporting.efd as efd
import lsst.ts.logging_and_reporting.exceptions as ex
import lsst.ts.logging_and_reporting.source_adapters as sad
import lsst.ts.logging_and_reporting.utils as ut


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
        warning=True,
        exclude_instruments=None,
    ):
        self.verbose = verbose
        self.warning = warning
        self.exclude_instruments = exclude_instruments or []
        ut.tic()
        # Load data for all needed sources for the selected dayobs range.
        self.nig_src = sad.NightReportAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            verbose=verbose,
            warning=warning,
        )
        self.exp_src = sad.ExposurelogAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            verbose=verbose,
            warning=warning,
        )
        self.nar_src = sad.NarrativelogAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            verbose=verbose,
            warning=warning,
        )
        self.cdb_src = cdb.ConsdbAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
            warning=warning,
        )
        self.alm_src = alm.Almanac(
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )
        # EfdClient is async so using it means its async to the top!
        self.efd_src = efd.EfdAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )

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

    def __str__(self):
        return f"{self.server_url}: {self.min_dayobs}, {self.max_dayobs}"

    def __repr__(self):
        return (
            f"AllSources(server_url={self.server_url!r}, "
            f"min_dayobs={self.min_dayobs!r}, "
            f"max_dayobs={self.max_dayobs!r})"
        )

    # see also:
    #   ~/sandbox/logrep/python/lsst/ts/logging_and_reporting/time_logs.py
    #   pandas.merge_asof
    #   pandas.timedelta_range
    #
    # alm_df, nig_df, exp_df, nar_df = allsrc.get_sources_time_logs()
    def get_sources_time_logs(self, verbose=False):
        """A time_log is a DF ordered and indexed with DatetimeIndex."""

        # Convert datefld column to Timestamp in "Timestamp" column
        def recs2df(recs, datefld):
            time_idx_name = "Time"
            # YYYY-MM-DD HH:MM
            times = pd.to_datetime([r[datefld] for r in recs], utc=True)
            index = pd.DatetimeIndex(times)

            df = pd.DataFrame(recs, index=index)
            df.index.name = time_idx_name
            df["Timestamp"] = times
            return df

        # Almanac
        alm_df = recs2df(self.alm_src.as_records(), "UTC")
        if verbose:
            print(f"Debug get_sources_time_logs: {alm_df.shape=}")

        # Night Report
        nig_df = recs2df(self.nig_src.records, self.nig_src.log_dt_field)
        if verbose:
            print(f"Debug get_sources_time_logs: {nig_df.shape=}")

        # NarrativeLog
        nar_df = recs2df(self.nar_src.records, self.nar_src.log_dt_field)
        if verbose:
            print(f"Debug get_sources_time_logs: {nar_df.shape=}")

        # ExposureLog
        exp_df = recs2df(self.exp_src.records, self.exp_src.log_dt_field)
        if verbose:
            print(f"Debug get_sources_time_logs: {exp_df.shape=}")

        recs = itertools.chain.from_iterable(self.exp_src.exposures.values())
        exp_detail_df = recs2df(recs, "timespan_begin")
        if verbose:
            print(f"Debug get_sources_time_logs: {exp_detail_df.shape=}")

        # self.cdb_src
        # The best time resolution is currently "day_obs"! (not good enuf)
        # This should be better when its available via TAP.

        # Source Records
        srecs = dict(ALM=alm_df)  # srecs[src_name] = [rec, ...]
        if not nig_df.empty:
            srecs["NIG"] = nig_df
        if not nar_df.empty:
            srecs["NAR"] = nar_df
        if not exp_df.empty:
            srecs["EXP"] = exp_df
        if not exp_detail_df.empty:
            # for all instruments that have exposures
            srecs["EXPDET"] = exp_detail_df
        return srecs

    @property
    def dayobs_range(self):
        return (self.min_dayobs, self.max_dayobs)

    # This will have to be async def night_tally_observation_gaps
    # if efd_src is used.
    def night_tally_observation_gaps(self, verbose=False):
        # observable is between 18deg twilights
        total_observable_hours = self.alm_src.night_hours
        used_instruments = set()
        for instrum, recs in self.exp_src.exposures.items():
            if instrum in self.exclude_instruments:
                continue
            if len(recs) > 0:
                used_instruments.add(instrum)
        if len(used_instruments) == 0:
            return {
                "": {
                    "Total night": ut.hhmmss(total_observable_hours),
                    "Idle time": ut.hhmmss(total_observable_hours),
                }
            }
        if verbose:
            print(f"DEBUG night_tally_observation_gaps: {used_instruments=} {self.exclude_instruments=}")

        instrument_tally = dict()  # d[instrument] = tally_dict

        # lost[day][lost_type] = totalTimeLost
        # lost = self.get_time_lost()

        # Use of nest_asyncio.apply will not give time to tasks
        # scheduled outside the nested run. This can potentially
        # leading to starvation too much time is spent on code
        # inside the nested run.
        # See also: https://sdiehl.github.io/gevent-tutorial/
        # #! nest_asyncio.apply()
        # #! loop = asyncio.get_event_loop()

        # slewTime (and probably others) are EXPECTED times, not ACTUAL.
        # To get actual, need to use TMAEvent or something similar.
        # targets = await self.efd_src.get_targets()  # a DataFrame
        # if verbose:
        #     print(
        #         f"AllSources().get_targets() got {len(targets)} targets "
        #         f"using date range {self.min_date} to {self.max_date}. "
        #     )

        # per Merlin: There is no practical way to get actual detector read
        # time.  He has done some experiments and inferred that it is
        # 2.3 seconds.  He recommends hardcoding the value.
        # Scot says use 2.41 per slack message in #consolidated-database
        readout_seconds = 2.41  # seconds per exposure
        readout_hours = readout_seconds / (60 * 60.0)  # hrs per exposure

        # Scot says care only about: LSSTComCam, LSSTCam and LATISS
        for instrument in used_instruments:
            records = self.exp_src.exposures[instrument]
            num_exposures = len(records)

            exposure_seconds = 0
            for rec in records:
                begin = dt.datetime.fromisoformat(rec["timespan_begin"])
                end = dt.datetime.fromisoformat(rec["timespan_end"])
                exposure_seconds += (end - begin).total_seconds()
            exposure_hours = exposure_seconds / (60 * 60.0)

            readout_hours = len(records) * readout_seconds / (60 * 60.0)

            # Merlin might add this to consdb or efd .... eventually
            # In the meantime, accept that we don't have SLEW times
            num_slews = pd.NA
            total_slew_seconds = pd.NA
            mean_slew = total_slew_seconds / num_slews
            slew_hours = total_slew_seconds / (60 * 60.0)

            # These need join between exposures and messages.
            # But in messages, they aren't reliable numbers anyhow.
            # TODO despite unreliability, use messages values.
            ltypes = set([r["time_lost_type"] for r in self.nar_src.records])
            ltime = defaultdict(int)
            print(f"Debug night_tally_observation_gaps: {ltypes=} {len(self.nar_src.records)=}")
            for t in ltypes:
                for r in self.nar_src.records:
                    ltime[t] += r["time_lost"]
            if "fault" in ltypes:
                loss_fault = ltime["fault"]  # hours
            else:
                loss_fault = pd.NA  # hours

            if "weather" in ltypes:
                loss_weather = ltime["weather"]  # hours
            else:
                loss_weather = pd.NA  # hours

            used_hours = exposure_hours + readout_hours
            if pd.notna(slew_hours):
                used_hours += slew_hours
            if pd.notna(loss_fault):
                used_hours += loss_fault
            if pd.notna(loss_weather):
                used_hours += loss_weather
            idle_hours = total_observable_hours - used_hours
            accounted_hours = used_hours + idle_hours

            instrument_tally[instrument] = {
                "Total Observable Night": ut.hhmmss(total_observable_hours),
                "Total Exposure": ut.hhmmss(exposure_hours),
                "Readout time(1)": ut.hhmmss(readout_hours),
                "Slew time(2)": ut.hhmmss(slew_hours),
                "Time loss due to fault": ut.hhmmss(loss_fault),
                "Time loss due to weather": ut.hhmmss(loss_weather),
                "Idle time": ut.hhmmss(idle_hours),
                "Number of exposures": num_exposures,
                "Number of slews": num_slews if pd.notna(num_slews) else "NA",
                "Mean Slew time": ut.hhmmss(mean_slew),
                "Total Accounted time": ut.hhmmss(accounted_hours),
            }

        # Composition to combine Exposure and Efd (blackboard)
        # ts_xml/.../sal_interfaces/Scheduler/Scheduler_Events.xml
        # https://ts-xml.lsst.io/sal_interfaces/Scheduler.html#slewtime
        # edf.get_targets() => "slewTime"                             # (d,g,h)
        tally_remarks = {
            "Total Observable Night": "time between 18 deg twilights",
            "Total Exposure": "Sum of exposure times",
            "Readout time(1)": "Sum of exposure readout times",
            "Slew time(2)": "Sum of slew times",
            "Time loss due to fault": "Sum of time lost due to faults (apx)",
            "Time loss due to weather": "Sum of time lost due to weather (apx)",
            "Idle time": "Sum of time doing 'nothing'",
            "Number of exposures": "",
            "Number of slews": "",
            "Mean Slew time": "",
            "Total Accounted time": "Total of above sums",
        }

        instrument_tally["Remarks"] = tally_remarks
        return instrument_tally

    def exposed_instruments(self):
        """Non-excluded instruments from in exposures from ExposureLog."""
        used_instruments = set()
        for instrum, recs in self.exp_src.exposures.items():
            if instrum in self.exclude_instruments:
                continue
            if len(recs) > 0:
                used_instruments.add(instrum)
        return used_instruments

    # modified from night_tally_observation_gaps()
    # Internal times in decimal hours. Render as HH:MM:SS
    def time_account(self, verbose=False):
        """Report on how instrument time is partitioned over the
        observing night."""

        # between 18deg twilights
        total_observable_hours = self.alm_src.night_hours
        used_instruments = self.exposed_instruments()
        if len(used_instruments) == 0:
            df = pd.DataFrame.from_records(
                [
                    {
                        "Total night": ut.hhmmss(total_observable_hours),
                        "Idle time": ut.hhmmss(total_observable_hours),
                    }
                ]
            )
            footnotes = {}
            return (df, footnotes)

        # Scot says use 2.41 sec/exposure in slack #consolidated-database
        exp_readout_hours = 2.41 / (60 * 60.0)  # hrs per exposure

        # Initial. Add Instrument columns.
        columns = ["Row", "Source", "Name", "Description"]
        account_records = [  # COLUMNS
            ("A", "<alm>", "Total Observable Night", "(between 18 deg twilights)"),
            ("B", "<nar>", "Total Exposure", "Total exposure time"),
            ("C", "const*<nar>", "Readout time(1)", "Total exposure readout time"),
            ("D", "(ConsDB later)", "Slew time(2)", "Total slew time"),
            ("E", "<nar>", "Time loss due to fault(3)", ""),
            ("F", "<nar>", "Time loss due to weather(4)", ""),
            ("G", "A-(B+C+D+F)", "Idle time", 'Time doing "nothing"'),
            ("H", "<nar>", "Number of exposures", ""),
            ("I", "D", "Number of slews", ""),
            ("J", "D", "Mean Slew time", ""),
            ("K", "B+C+D+G", "Total Accounted for time", ""),
        ]
        footnotes = {
            "1": (
                "There is no practical way to get detector read-out "
                "time. A value of 2.41 seconds per exposure is used."
            ),
            "2": (
                "There is no simple way to get slew times. We expect SlewTime "
                "to find its way into the Consolidated Database eventually"
            ),
            "3": (
                "A fault loss is assumed to be associated with a specific "
                "Instrument so is counted as applying to just that instrument. "
                "The Instrument for the fault loss is derived from "
                "a 'component' field that MIGHT contain a the Telescope name. "
                "The derived Instrument is different for records added "
                "on or after 2025-01-20. "
                "If a particular fault loss cannot be associated with a "
                "known telescope, that loss it not counted here at all. "
                "If the Instrument associated with a fault lost "
                "is not one that has exposures in the above table, "
                "it is not counted here. "
                "The Telescope -> Instrument mapping used is: "
                "AuxTel->LATISS, "
                "(MainTel,Simonyi)->LSSTComCam (before 2025-01-20), "
                "(MainTel,Simonyi)->LSSTCam (on/after 2025-01-20), "
                "For purposes of counting this loss, it does not matter "
                "if the associated exposure was on sky or not."
            ),
            "4": (
                "A weather loss is not associated with a specific Instrument "
                "so it is counted as applying to all instruments. "
                "Different telescopes might have different weather "
                "(e.g. clouds) so this might be wrong occasionaly. "
                "For purposes of counting this loss, it does not matter "
                "if the associated exposure was on sky or not."
            ),
        }

        df = pd.DataFrame.from_records(account_records, columns=columns).set_index("Row")

        # Calc time accounting for each Instrument.
        # "Total Observable Night" same for all instruments.
        for instrument in used_instruments:
            exp_records = self.exp_src.exposures[instrument]

            exposure_seconds = 0
            for rec in exp_records:
                begin = dt.datetime.fromisoformat(rec["timespan_begin"])
                end = dt.datetime.fromisoformat(rec["timespan_end"])
                exposure_seconds += (end - begin).total_seconds()
            exposure_hours = exposure_seconds / (60 * 60.0)
            readout_hours = len(exp_records) * exp_readout_hours

            # Merlin might add this to consdb or efd .... eventually
            # In the meantime, accept that we don't have SLEW times
            num_slews = pd.NA
            total_slew_seconds = pd.NA
            mean_slew = total_slew_seconds / num_slews
            slew_hours = total_slew_seconds / (60 * 60.0)

            # Calc observable time lost per instrument from Fault and Weather.
            # Its possible that both Fault and Weather affect same or
            # overlapping time. Also, we don't know if Fault is for specific
            # instrument.
            # In both cases we may double count!
            # TODO despite unreliability, use messages values.
            ltypes = set([r["time_lost_type"] for r in self.nar_src.records])
            ltime = defaultdict(int)
            for lt in ltypes:
                for r in self.nar_src.records:
                    if r["instrument"] == instrument:
                        ltime[lt] += r["time_lost"]
            # Loss due to FAULT
            if "fault" in ltypes:
                loss_fault = ltime["fault"]  # hours
            else:
                loss_fault = 0
            # Loss due to WEATHER
            if "weather" in ltypes:
                loss_weather = ltime["weather"]  # hours
            else:
                loss_weather = 0

            act_hours = exposure_hours + readout_hours
            if pd.notna(slew_hours):
                act_hours += slew_hours
            if pd.notna(loss_fault):
                act_hours += loss_fault
            if pd.notna(loss_weather):
                act_hours += loss_weather
            idle_hours = total_observable_hours - act_hours

            # Stuff values
            def rt(hours):  # Render Time
                return ut.hhmmss(hours)
                # return f'{hours:.2f}'

            df.loc["A", instrument] = rt(total_observable_hours)
            df.loc["B", instrument] = rt(exposure_hours)
            df.loc["C", instrument] = rt(readout_hours)
            df.loc["D", instrument] = rt(slew_hours)
            df.loc["E", instrument] = rt(loss_fault)
            df.loc["F", instrument] = rt(loss_weather)
            df.loc["G", instrument] = rt(idle_hours)
            df.loc["H", instrument] = len(exp_records)
            df.loc["I", instrument] = num_slews
            df.loc["J", instrument] = rt(mean_slew)
            df.loc["K", instrument] = rt(
                exposure_hours
                + readout_hours
                + (0 if pd.isna(slew_hours) else slew_hours)
                + loss_fault
                + loss_weather
                + idle_hours
            )
        return (
            df[["Name"] + list(used_instruments) + ["Description", "Source"]],
            footnotes,
        )
        # END time_account()

    # see source_record_counts()
    def records_per_source(self):
        sources = [
            # self.alm_src,
            self.nig_src,
            self.exp_src,
            self.nar_src,
            self.efd_src,
        ]
        res = {
            src.service: {
                endpoint: (ed["number_of_records"], ed["endpoint_url"]) for endpoint, ed in src.status.items()
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

    def source_record_counts(self):
        return {stat["Endpoint"]: stat["Records"] for stat in self.get_data_status()}

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
            day_tl[day]["fault"] = sum([r["time_lost"] for r in day_grp if "fault" == r["time_lost_type"]])
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
        field_name_title = field_name.title().replace("_", " ")
        for field in field_values:
            for eflag in eflag_values:
                # Initialize to zeros
                counter = Counter({f: 0 for f in eflag_values})
                counter.update([r["exposure_flag"] for r in records if r[field_name] == field])
            table_recs[field][field_name_title] = gen_link(field)
            table_recs[field].update(dict(counter))
            # User want this?: counter.update(dict(total=counter.total()))
        if table_recs:
            df = pd.DataFrame.from_records(
                list(table_recs.values()),
                index=list(table_recs.keys()),
            )
            df.sort_index(inplace=True)
            # Add Total row
            tot_df = pd.DataFrame(
                [*df.values, ["Total", *df.sum(numeric_only=True).values]],
                columns=df.columns,
            )
        else:
            tot_df = pd.DataFrame()

        return tot_df

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

    # Similar to ExposurelogAdapter.exposure_detail but includes consdb
    def exposure_detail_all_src(
        self,
        instrument,
        science_program=None,
        observation_type=None,
        observation_reason=None,
    ):
        # recs are list of dicts. Dict keys are essentially DB column names
        crecs = self.cdb_src.exposures[instrument]

        # Filter exposures from exposurelog
        program = science_program and science_program.lower()
        otype = observation_type and observation_type.lower()
        reason = observation_reason and observation_reason.lower()
        erecs = [
            r
            for r in self.exp_src.exposures[instrument]
            if ((program is None) or (r["science_program"].lower() == program))
            and ((otype is None) or (r["observation_type"].lower() == otype))
            and ((reason is None) or (r["observation_reason"].lower() == reason))
        ]
        if self.verbose:
            print(
                f"allsrc.exposure_detail({instrument}, "
                f"{science_program=},{observation_type=},{observation_reason=}):"
            )
            print(
                f"{program=} {otype=} {reason=} "
                f"pre-filter={len(self.exp_src.exposures[instrument])} "
                f"post-filter={len(erecs)}"
            )

        if 0 == len(crecs) + len(erecs):
            if self.warning:
                msg = "No records found for ConsDB or ExposureLog "
                msg += f"for {instrument=}."
                warnings.warn(msg, category=ex.NoRecordsWarning, stacklevel=2)
            return pd.DataFrame()  # empty

        # Join records by c.exposure_id = e.id (using Pandas)
        cdf = pd.DataFrame(crecs)
        edf = pd.DataFrame(erecs)
        if self.verbose:
            print(f"{cdf.shape=} {edf.shape=} ")
            print(f"{sorted(cdf.columns)=}")
            print(f"{sorted(edf.columns)=}")

        # ConsDB doesn't have exposures for all instruments
        if "exposure_id" in cdf:
            if "id" in edf:
                df = cdf.set_index("exposure_id", drop=False).join(
                    edf.set_index("id", drop=False),
                    how="inner",
                    lsuffix="_CDB",
                    rsuffix="_EXP",
                )
            else:  # yes CDF, no EDF
                df = cdf.set_index("exposure_id", drop=False)

        else:  # no CDF
            if "id" in edf:
                # no CDF, yes EDF
                df = edf.set_index("id", drop=False)
            else:
                # no CDF, no EDF
                pass  # empty DF returned above

        fields = {
            "air_temp",
            "airmass",
            "altitude",
            "azimuth",
            "band",
            "dimm_seeing",
            "exp_time",
            "exposure_flag",
            "exposure_id",
            "exposure_name",
            "high_snr_source_count_median",
            "instrument",  # not requested
            "obs_id",
            "observation_reason",
            "observation_type",
            "psf_trace_radius_delta_median",
            "s_dec",
            "s_ra",
            "science_program",
            "seeing_zenith_500nm_median",
            "seq_num",
            "sky_angle",  # not requested
            "sky_bg_median",
            "sky_rotation",
            "target_name",  # not requested
            "timespan_begin",
            "zero_point_median",
            # ################################### Available but ignored
            # 'day_obs_CDB',
            # 'day_obs_EXP',
            # 'exposure_time',
            "group_name",
            # 'obs_start',
            "target_name",
            # 'timespan_end',
            # 'tracking_dec',
            # 'tracking_ra',
            # 'visit_id',
        }
        labels = {
            "air_temp": "Outside Air Temp",
            "airmass": "Airmass",
            "altitude": "Altitude",
            "azimuth": "Azimuth",
            "band": "Filter Bandpass",
            "dimm_seeing": "Dimm Seeing",
            "exp_time": "Exp Time",
            "exposure_flag": "Exposure Flag",
            "exposure_id": "Exposure Id",
            "exposure_name": "Exposure Name",
            "high_snr_source_count_median": "Source Counts",
            "instrument": "Instrument",
            "obs_id": "Obs Id",
            "observation_reason": "Observation Reason",
            "observation_type": "Observation Type",
            "psf_trace_radius_delta_median": "Psf Trace Radius Delta Median",
            "s_dec": "Spatial Dec",
            "s_ra": "Spatial Ra",
            "science_program": "Science Program",
            "seeing_zenith_500nm_median": "Seeing Zenith 500Nm Median",
            "seq_num": "Seq Num",
            "sky_angle": "Sky Angle",
            "sky_bg_median": "Sky Background",
            "sky_rotation": "Sky Rotation",
            "target_name": "Target Name",
            "timespan_begin": "Timespan Begin",
            "zero_point_median": "Photometric Zero Points",
        }

        used_fields = set(sorted(df.columns.to_list())) & fields

        # #! df = ut.wrap_dataframe_columns(df[fields])
        # #! df.columns = df.columns.str.title()
        if self.verbose:
            print(f"Debug allsrc.exposure_detail {used_fields=} {sorted(labels.keys())=}")
        if self.warning:
            if used_fields < fields:
                msg = "Some requested fields are not available. "
                msg += f"Requested fields not used: {fields - used_fields}"
                warnings.warn(msg, category=ex.NotAvailWarning, stacklevel=2)
        df = df[list(used_fields)].rename(columns=labels, errors="ignore")
        return df

    # np.cumsum([r['message_text'].count('\n')
    #   for r in allsrc.nar_src.records])
    def nar_split_messages(self, max_head_lines=26):
        """Split the narrativelog messages into two lists.
        The HEAD contains records that contain <=r NUM_HEAD_LINES
        lines of text.  The TAIL contains all the rest of the records.
        The intention is to display HEAD in the Digest, and HEAD + TAIL in a
        Detail page.  If UI supports, Digest could contain HEAD + "more" button
        that will turn on/off visiblity of TAIL."""
        num_lines = 0
        records = self.nar_src.records
        print(f"{max_head_lines=}")
        for idx, rec in enumerate(records, 0):
            num_lines += rec["message_text"].count("\n")
            if num_lines > max_head_lines:
                head = records[:idx]
                tail = records[idx:]
                return (head, tail)

    # END class AllSources


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
    facets = {f: set([str(r[f]) for r in records if not isinstance(r[f], list)]) for f in facflds}

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
    facets, ignored = get_facets(records, fieldnames=fieldnames, ignore_fields=ignore_fields)
    fc = {k: len(v) for k, v in facets.items()}
    fc["total"] = len(records)
    return fc
