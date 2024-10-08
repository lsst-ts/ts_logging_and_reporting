import datetime as dt

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
        self.efd_src = efd.EfdAdapter(
            server_url=server_url,
            min_dayobs=min_dayobs,
            max_dayobs=max_dayobs,
        )
        # This space for rent by ConsDB

        # Get the common min/max date/dayobs from just one source.
        # They are the same for all of them.
        self.max_date = self.nig_src.max_date
        self.min_date = self.nig_src.min_date
        self.max_dayobs = self.nig_src.max_dayobs
        self.min_dayobs = self.nig_src.min_dayobs

    # END init

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
    async def night_tally_observation_gaps(self, verbose=True):

        instrument_tally = dict()  # d[instrument] = tally_dict
        almanac = alm.Almanac(dayobs=self.min_dayobs)
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

        for instrument, records in self.exp_src.exposures.items():
            exposure_seconds = 0
            for rec in records:
                begin = dt.datetime.fromisoformat(rec["timespan_begin"])
                end = dt.datetime.fromisoformat(rec["timespan_end"])
                exposure_seconds += (end - begin).total_seconds()
            num_exposures = len(records)
            exposure_hrs = exposure_seconds / (60 * 60.0)
            slew_hrs = total_slew_seconds / (60 * 60)
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
                "Total Detector Read (HH:MM:SS)": "NA",  # (e) UNKNOWN SOURCE
                "Mean Detector Read (HH:MM:SS)": "NA",  # (f=e/c)
                "Total Slew (HH:MM:SS)": hhmmss(slew_hrs),  # (g)
                "Mean Slew (HH:MM:SS)": hhmmss(slew_hrs / num_slews),  # (g/d)
                "Total Idle (HH:MM:SS)": hhmmss(idle_hrs),  # (i=a-b-e-g)
            }

        # get_detector_reads()??  # UNKNOWN SOURCE

        # Composition to combine Exposure and Efd (blackboard)
        # ts_xml/.../sal_interfaces/Scheduler/Scheduler_Events.xml
        # https://ts-xml.lsst.io/sal_interfaces/Scheduler.html#slewtime
        # edf.get_targets() => "slewTime"                             # (d,g,h)
        return instrument_tally
