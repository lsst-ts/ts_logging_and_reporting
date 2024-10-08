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
    async def night_tally_observation_gaps(self, dayobs):
        instrument_tally = dict()  # d[instrument] = tally_dict
        almanac = alm.Almanac(dayobs=dayobs)
        total_observable_hours = almanac.night_hours

        targets = await self.efd_src.get_targets()
        num_slews = targets[["slewTime"]].astype(bool).sum(axis=0).squeeze()
        total_slew_seconds = targets[["slewTime"]].sum().squeeze()

        for instrument, records in self.exp_src.exposures.items():
            exposure_seconds = 0
            for rec in records:
                begin = dt.datetime.fromisoformat(rec["timespan_begin"])
                end = dt.datetime.fromisoformat(rec["timespan_end"])
                exposure_seconds += (end - begin).total_seconds()
            num_exposures = len(records)
            exposure_hours = exposure_seconds / (60 * 60.0)
            slew_hours = total_slew_seconds / (60 * 60)
            idle_hours = (
                total_observable_hours
                - exposure_hours
                # - detector_read_hours
                - slew_hours
            )
            instrument_tally[instrument] = {
                "Total Night hours": hhmmss(total_observable_hours),  # (a)
                "Total Exposure hours": hhmmss(exposure_hours),  # (b)
                "Number of exposures": num_exposures,  # (c)
                "Number of slews": num_slews,  # (d)
                "Total Detector Read hours": "NA",  # (e) UNKNOWN SOURCE
                "Mean Detector Read hours": "NA",  # (f=e/c)
                "Total Slew hours": hhmmss(slew_hours),  # (g)
                "Mean Slew hours": hhmmss(slew_hours / num_slews),  # (h=g/d)
                "Total Idle hours": hhmmss(idle_hours),  # (i=a-b-e-g)
            }

        # get_detector_reads()??  # UNKNOWN SOURCE

        # Composition to combine Exposure and Efd (blackboard)
        # ts_xml/.../sal_interfaces/Scheduler/Scheduler_Events.xml
        # https://ts-xml.lsst.io/sal_interfaces/Scheduler.html#slewtime
        # edf.get_targets() => "slewTime"                             # (d,g,h)
        return instrument_tally
