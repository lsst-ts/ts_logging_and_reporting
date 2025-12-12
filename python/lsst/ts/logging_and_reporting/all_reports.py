import pandas as pd
from IPython.display import display

import lsst.ts.logging_and_reporting.reports as rep


class AllReports:
    """Container for all Report instances used by LogRep."""

    def __init__(self, *, allsrc=None):
        # Associate all specialized reports with their sepcialized adapter
        self.alm_rep = rep.AlmanacReport(adapter=allsrc.alm_src)
        self.nig_rep = rep.NightlyLogReport(adapter=allsrc.nig_src)
        self.exp_rep = rep.ExposurelogReport(adapter=allsrc.exp_src)
        self.nar_rep = rep.NarrativelogReport(adapter=allsrc.nar_src)

    def plot_observation_gap_rollup(self, rollup):
        if not rollup:
            return

        for instrument, day_gaps in rollup.items():
            x, y = zip(*day_gaps.items())
            df = pd.DataFrame(dict(day=x, minutes=y))
            df.plot.bar(x="day", y="minutes", title=f"{instrument=!s}")

    def plot_observation_gap_detail(self, detail):
        if not detail:
            return

        display(detail)

        # TODO modify for multi-attribute gaps
        #  # for instrument, day_gaps in detail.items():
        #  #     x, y = zip(*day_gaps.items())
        #  #     df = pd.DataFrame(dict(day=x, minutes=y))
        #  #     df.plot.bar(x="day", y="minutes", title=f"{instrument=!s}")
